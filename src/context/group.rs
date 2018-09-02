//! Group buffer representations for `Reducer` stages.

/// Represents a value grouping for a current key.
#[derive(Debug)]
pub struct Group {
    key: String,
    values: Vec<String>,
}

impl Group {
    /// Creates a new unset `Group`.
    pub fn new() -> Self {
        Group {
            key: "".to_owned(),
            values: Vec::new(),
        }
    }

    /// Checks whether this `Group` is unset.
    pub fn is_unset(&self) -> bool {
        self.key == ""
    }

    /// Returns a reference to the current key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Pushes a value to the internal buffer.
    pub fn push(&mut self, value: String) {
        self.values.push(value);
    }

    /// Resets the internal buffer on a key change.
    ///
    /// On reset, the old key and buffer are returned to the caller
    /// for further processing. The internal buffer is simply drained,
    /// rather than replaced with a new `Vec` in memory.
    pub fn reset(&mut self, key: &str) -> (String, Vec<String>) {
        let k = self.key.clone();
        let v = self.values.drain(..).collect();

        self.key = key.to_owned();

        (k, v)
    }

    /// Returns a reference to the current value buffer.
    pub fn values(&self) -> &Vec<String> {
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

        group.push("one".to_owned());
        group.push("two".to_owned());
        group.push("three".to_owned());

        assert_eq!(group.values().len(), 3);

        let (key, values) = group.reset("new_key");

        assert_eq!(key, "");
        assert_eq!(values[0], "one");
        assert_eq!(values[1], "two");
        assert_eq!(values[2], "three");

        let new_key = group.key();
        let new_values = group.values();

        assert_eq!(new_key, "new_key");
        assert_eq!(new_values.len(), 0);
    }
}
