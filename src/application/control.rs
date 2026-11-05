//! Streamed stdin: a bounded line queue per streamed step, and the router
//! that feeds those queues from the control input.
//!
//! Control lines are secrets-in-transit (they feed a process's stdin): they
//! are never written to events, reports or logs. Warnings name the task only.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use futures_util::Stream;

use crate::application::ports::EventSink;
use crate::domain::{Event, TaskId};

/// Longest control line accepted (the whole NDJSON object, in bytes).
pub const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;
/// Lines queued per task before new ones are dropped.
pub const CONTROL_QUEUE_LINES: usize = 256;

/// Why a chunk was not queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    Full,
    Closed,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Vec<u8>>,
    /// Set by the sender (no more input) or by the receiver (gone).
    closed: bool,
    waker: Option<Waker>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Producer half of a bounded input queue. Dropping it ends the stream.
pub struct InputSender {
    queue: Arc<Mutex<Queue>>,
    capacity: usize,
}

/// Consumer half: a stream of byte chunks to write to a process's stdin. It
/// ends (EOF) once the sender is closed and the queue is drained.
pub struct InputReceiver {
    queue: Arc<Mutex<Queue>>,
}

/// Creates a bounded queue of at most `capacity` pending chunks.
pub fn input_channel(capacity: usize) -> (InputSender, InputReceiver) {
    let queue = Arc::new(Mutex::new(Queue::default()));
    (
        InputSender {
            queue: Arc::clone(&queue),
            capacity,
        },
        InputReceiver { queue },
    )
}

impl InputReceiver {
    /// A stream that is already at EOF.
    pub fn closed() -> Self {
        input_channel(1).1
    }
}

impl InputSender {
    /// Never blocks: a full queue drops the chunk.
    pub fn try_send(&self, chunk: Vec<u8>) -> Result<(), SendError> {
        let mut q = lock(&self.queue);
        if q.closed {
            return Err(SendError::Closed);
        }
        if q.items.len() >= self.capacity {
            return Err(SendError::Full);
        }
        q.items.push_back(chunk);
        if let Some(waker) = q.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Ends the stream once the queued chunks are consumed.
    pub fn close(&self) {
        let mut q = lock(&self.queue);
        q.closed = true;
        if let Some(waker) = q.waker.take() {
            waker.wake();
        }
    }
}

impl Drop for InputSender {
    fn drop(&mut self) {
        self.close();
    }
}

impl Stream for InputReceiver {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<u8>>> {
        let mut q = lock(&self.queue);
        if let Some(chunk) = q.items.pop_front() {
            return Poll::Ready(Some(chunk));
        }
        if q.closed {
            return Poll::Ready(None);
        }
        q.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for InputReceiver {
    fn drop(&mut self) {
        let mut q = lock(&self.queue);
        q.closed = true;
        q.items.clear();
    }
}

/// One decoded unit of the control input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    /// Text for a task's streamed stdin (a newline is appended on delivery).
    Line { task: String, line: String },
    /// A line longer than [`MAX_CONTROL_LINE_BYTES`], discarded unread.
    Oversized,
    /// A line that is not a valid control object.
    Malformed,
}

#[derive(Default)]
struct Routes {
    streams: HashMap<String, InputSender>,
    /// The control input ended: every stream is at EOF, later ones start so.
    closed: bool,
}

/// Registry of the streamed steps currently running, keyed by task id.
/// Cheap to clone; clones share state.
#[derive(Clone, Default)]
pub struct ControlRouter {
    routes: Arc<Mutex<Routes>>,
}

impl ControlRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the streamed step now running for `task` and returns the
    /// stream feeding its stdin. After [`close_all`](Self::close_all) the
    /// stream is already at EOF.
    pub fn open(&self, task: &TaskId) -> InputReceiver {
        let mut routes = lock(&self.routes);
        let (tx, rx) = input_channel(CONTROL_QUEUE_LINES);
        if routes.closed {
            return rx;
        }
        routes.streams.insert(task.as_str().to_owned(), tx);
        rx
    }

    /// Unregisters `task`'s streamed step (it ended); later lines for the
    /// task are dropped with a warning.
    pub fn release(&self, task: &TaskId) {
        lock(&self.routes).streams.remove(task.as_str());
    }

    /// Control input closed: EOF to every open stream and every later one.
    pub fn close_all(&self) {
        let mut routes = lock(&self.routes);
        routes.closed = true;
        routes.streams.clear();
    }

    /// Delivers one message, or emits a warning when it cannot be delivered.
    /// Never blocks and never echoes line content.
    pub fn handle<S: EventSink>(&self, sink: &S, message: ControlMessage) {
        let warn = |task: Option<&TaskId>, message: &str| {
            sink.emit(&Event::Warning { task, message });
        };
        match message {
            ControlMessage::Oversized => warn(
                None,
                &format!("control line dropped: longer than {MAX_CONTROL_LINE_BYTES} bytes"),
            ),
            ControlMessage::Malformed => warn(
                None,
                "control line dropped: expected {\"task\":\"<id>\",\"line\":\"<text>\"}",
            ),
            ControlMessage::Line { task, line } => {
                // Ids that cannot name a task are not echoed back either.
                let Ok(id) = TaskId::new(task) else {
                    warn(None, "control line dropped: invalid task id");
                    return;
                };
                let mut bytes = line.into_bytes();
                bytes.push(b'\n');
                let sent = match lock(&self.routes).streams.get(id.as_str()) {
                    Some(tx) => tx.try_send(bytes),
                    None => Err(SendError::Closed),
                };
                match sent {
                    Ok(()) => {}
                    Err(SendError::Full) => warn(
                        Some(&id),
                        &format!(
                            "control line dropped: {CONTROL_QUEUE_LINES} lines already queued"
                        ),
                    ),
                    Err(SendError::Closed) => warn(
                        Some(&id),
                        "control line dropped: task has no streaming step running",
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[test]
    fn queue_is_bounded_and_ends_after_close() {
        let (tx, mut rx) = input_channel(2);
        assert_eq!(tx.try_send(vec![1]), Ok(()));
        assert_eq!(tx.try_send(vec![2]), Ok(()));
        assert_eq!(tx.try_send(vec![3]), Err(SendError::Full));
        drop(tx);
        let got = futures_util::future::FutureExt::now_or_never(async {
            let mut all = Vec::new();
            while let Some(c) = rx.next().await {
                all.push(c);
            }
            all
        });
        assert_eq!(got, Some(vec![vec![1], vec![2]]));
    }

    #[test]
    fn dropped_receiver_closes_the_sender() {
        let (tx, rx) = input_channel(2);
        drop(rx);
        assert_eq!(tx.try_send(vec![1]), Err(SendError::Closed));
    }
}
