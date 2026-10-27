//! Bounded output handling. Containers may print gigabytes; the dispatcher
//! runs in ~1 GiB of RAM, so every buffer here has a hard cap.

/// Keeps only the last `capacity` bytes written to it.
#[derive(Debug, Clone)]
pub struct TailBuffer {
    buf: Vec<u8>,
    capacity: usize,
    total: u64,
}

impl TailBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::new(),
            capacity,
            total: 0,
        }
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.total += chunk.len() as u64;
        if self.capacity == 0 {
            return;
        }
        if chunk.len() >= self.capacity {
            self.buf.clear();
            self.buf
                .extend_from_slice(&chunk[chunk.len() - self.capacity..]);
            return;
        }
        let overflow = (self.buf.len() + chunk.len()).saturating_sub(self.capacity);
        if overflow > 0 {
            self.buf.drain(..overflow);
        }
        self.buf.extend_from_slice(chunk);
    }

    /// Total bytes ever pushed, including discarded ones.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }

    pub fn is_truncated(&self) -> bool {
        self.total > self.buf.len() as u64
    }

    /// Lossy UTF-8 view of the retained tail.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

/// Re-assembles arbitrary byte chunks into lines. Lines longer than
/// `max_line` are emitted in `max_line`-sized pieces so a missing newline can
/// never grow memory unbounded.
#[derive(Debug, Clone)]
pub struct LineSplitter {
    pending: Vec<u8>,
    max_line: usize,
}

impl LineSplitter {
    pub fn new(max_line: usize) -> Self {
        Self {
            pending: Vec::new(),
            max_line: max_line.max(1),
        }
    }

    /// Feeds a chunk, calling `emit` once per complete line (without the
    /// trailing `\n` / `\r\n`).
    pub fn push(&mut self, mut chunk: &[u8], mut emit: impl FnMut(&[u8])) {
        while !chunk.is_empty() {
            match chunk.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    let (line, rest) = chunk.split_at(pos);
                    self.append(line, &mut emit);
                    let mut full = std::mem::take(&mut self.pending);
                    if full.last() == Some(&b'\r') {
                        full.pop();
                    }
                    emit(&full);
                    full.clear();
                    self.pending = full;
                    chunk = &rest[1..];
                }
                None => {
                    self.append(chunk, &mut emit);
                    chunk = &[];
                }
            }
        }
    }

    /// Emits whatever is left without a trailing newline.
    pub fn finish(&mut self, mut emit: impl FnMut(&[u8])) {
        if !self.pending.is_empty() {
            emit(&self.pending);
            self.pending.clear();
        }
    }

    fn append(&mut self, mut bytes: &[u8], emit: &mut impl FnMut(&[u8])) {
        while self.pending.len() + bytes.len() > self.max_line {
            let take = self.max_line - self.pending.len();
            self.pending.extend_from_slice(&bytes[..take]);
            emit(&self.pending);
            self.pending.clear();
            bytes = &bytes[take..];
        }
        self.pending.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(splitter: &mut LineSplitter, chunks: &[&[u8]]) -> Vec<String> {
        let mut out = Vec::new();
        for chunk in chunks {
            splitter.push(chunk, |l| out.push(String::from_utf8_lossy(l).into_owned()));
        }
        splitter.finish(|l| out.push(String::from_utf8_lossy(l).into_owned()));
        out
    }

    #[test]
    fn tail_keeps_last_bytes() {
        let mut t = TailBuffer::new(5);
        t.push(b"abc");
        assert!(!t.is_truncated());
        t.push(b"defg");
        assert_eq!(t.to_string_lossy(), "cdefg");
        t.push(b"0123456789");
        assert_eq!(t.to_string_lossy(), "56789");
        assert_eq!(t.total_bytes(), 17);
        assert!(t.is_truncated());
    }

    #[test]
    fn tail_with_zero_capacity_only_counts() {
        let mut t = TailBuffer::new(0);
        t.push(b"abc");
        assert_eq!(t.to_string_lossy(), "");
        assert_eq!(t.total_bytes(), 3);
    }

    #[test]
    fn splitter_joins_chunks_across_boundaries() {
        let mut s = LineSplitter::new(1024);
        assert_eq!(
            lines(&mut s, &[b"hel", b"lo\nwor", b"ld\r\n\nlast"]),
            ["hello", "world", "", "last"]
        );
    }

    #[test]
    fn splitter_breaks_overlong_lines() {
        let mut s = LineSplitter::new(4);
        assert_eq!(
            lines(&mut s, &[b"abcdefghij\nxy"]),
            ["abcd", "efgh", "ij", "xy"]
        );
    }
}
