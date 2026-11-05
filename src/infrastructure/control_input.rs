//! Control input: NDJSON `{"task":"<id>","line":"<text>"}` objects, one per
//! line, read with a hard cap on memory per line.
//!
//! Decoded lines are secrets-in-transit: nothing here logs or echoes their
//! content, and parse errors are collapsed to [`ControlMessage::Malformed`]
//! because serde's messages can quote the offending input.

use std::io;
use std::path::Path;

use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio::net::unix::pipe;

use crate::application::{ControlMessage, MAX_CONTROL_LINE_BYTES};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlDto {
    task: String,
    line: String,
}

/// Opens the control input at `path` (a pipe such as `/dev/fd/3`, or a FIFO).
/// Opening never blocks, whether or not a writer is connected yet.
pub fn open_control(path: &Path) -> io::Result<ControlReader<BufReader<pipe::Receiver>>> {
    let receiver = pipe::OpenOptions::new().open_receiver(path)?;
    Ok(ControlReader::new(BufReader::with_capacity(
        8 * 1024,
        receiver,
    )))
}

/// Splits a byte stream into control messages, holding at most
/// [`MAX_CONTROL_LINE_BYTES`] of a line in memory.
pub struct ControlReader<R> {
    reader: R,
}

impl<R: AsyncBufRead + Unpin> ControlReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Next message, or `None` once the input is closed. Blank lines are
    /// skipped; a final line without a newline still counts.
    pub async fn next_message(&mut self) -> io::Result<Option<ControlMessage>> {
        let mut line = Vec::new();
        let mut oversized = false;
        let mut seen = false;
        loop {
            let chunk = self.reader.fill_buf().await?;
            if chunk.is_empty() {
                return Ok(seen.then(|| decode(&line, oversized)).flatten());
            }
            let end = chunk.iter().position(|&b| b == b'\n');
            let piece = &chunk[..end.unwrap_or(chunk.len())];
            seen |= !piece.is_empty();
            if line.len() + piece.len() > MAX_CONTROL_LINE_BYTES {
                // Keep reading to the newline, but stop buffering.
                oversized = true;
                line.clear();
            } else if !oversized {
                line.extend_from_slice(piece);
            }
            let used = end.map_or(chunk.len(), |i| i + 1);
            self.reader.consume(used);
            if end.is_some() {
                if let Some(message) = seen.then(|| decode(&line, oversized)).flatten() {
                    return Ok(Some(message));
                }
                line.clear();
                oversized = false;
                seen = false;
            }
        }
    }
}

fn decode(line: &[u8], oversized: bool) -> Option<ControlMessage> {
    if oversized {
        return Some(ControlMessage::Oversized);
    }
    if line.iter().all(u8::is_ascii_whitespace) {
        return None;
    }
    Some(match serde_json::from_slice::<ControlDto>(line) {
        Ok(ControlDto { task, line }) => ControlMessage::Line { task, line },
        Err(_) => ControlMessage::Malformed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_all(input: &[u8]) -> Vec<ControlMessage> {
        let mut reader = ControlReader::new(input);
        let mut out = Vec::new();
        while let Some(m) = reader.next_message().await.unwrap() {
            out.push(m);
        }
        out
    }

    fn line(task: &str, text: &str) -> ControlMessage {
        ControlMessage::Line {
            task: task.into(),
            line: text.into(),
        }
    }

    #[tokio::test]
    async fn decodes_lines_and_flags_bad_ones() {
        let input = b"{\"task\":\"a\",\"line\":\"hi \\\"x\\\"\"}\n\nnot json\n\
                      {\"task\":\"b\"}\n{\"task\":\"b\",\"line\":\"tail\"}";
        assert_eq!(
            read_all(input).await,
            [
                line("a", "hi \"x\""),
                ControlMessage::Malformed,
                ControlMessage::Malformed,
                line("b", "tail"),
            ]
        );
    }

    #[tokio::test]
    async fn drops_oversized_lines_and_resumes() {
        let mut input = vec![b'x'; MAX_CONTROL_LINE_BYTES + 1];
        input.extend_from_slice(b"\n{\"task\":\"a\",\"line\":\"ok\"}\n");
        assert_eq!(
            read_all(&input).await,
            [ControlMessage::Oversized, line("a", "ok")]
        );
    }

    #[tokio::test]
    async fn empty_input_is_eof() {
        assert!(read_all(b"").await.is_empty());
    }
}
