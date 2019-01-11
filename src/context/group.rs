//! Group buffer representations for `Reducer` stages.

/// Represents a value grouping for a current key.
#[derive(Debug)]
pub struct Group {
    key: Vec<u8>,
    values: Vec<Vec<u8>>,
}

impl Group {
    /// Creates a new unset `Group`.
    pub fn new() -> Self {
        Group {
            key: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Checks whether this `Group` is unset.
    pub fn is_unset(&self) -> bool {
        self.key.is_empty()
    }

    /// Returns a reference to the current key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Pushes a value to the internal buffer.
    pub fn push(&mut self, value: Vec<u8>) {
        self.values.push(value);
    }

    /// Resets the internal buffer on a key change.
    ///
    /// On reset, the old key and buffer are returned to the caller
    /// for further processing. The internal buffer is simply drained,
    /// rather than replaced with a new `Vec` in memory.
    pub fn reset(&mut self, key: &[u8]) -> (Vec<u8>, Vec<Vec<u8>>) {
        let k = self.key.clone();
        let v = self.values.drain(..).collect();

        self.key = key.to_vec();

        (k, v)
    }

    /// Returns a reference to the current value buffer.
    pub fn values(&self) -> &Vec<Vec<u8>> {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_creation() {
        let group = Group::new();

        assert!(group.is_unset());
    }

    #[test]
    fn test_group_reset() {
        let mut group = Group::new();

        group.push(b"one".to_vec());
        group.push(b"two".to_vec());
        group.push(b"three".to_vec());

        assert_eq!(group.values().len(), 3);

        let (key, values) = group.reset(b"new_key");

        assert_eq!(key, b"");
        assert_eq!(values[0], b"one");
        assert_eq!(values[1], b"two");
        assert_eq!(values[2], b"three");

        let new_key = group.key();
        let new_values = group.values();

        assert_eq!(new_key, b"new_key");
        assert_eq!(new_values.len(), 0);
    }
}
