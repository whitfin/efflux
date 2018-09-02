//! Offset bindings to provide byte offsets for `Mapper` stages.

/// Offset structure to allow tracking of the current byte.
///
/// This offers little more than abstraction over the byte
/// offset being tracked manually; however the strong typing
/// allows `Offset` to be added to a `Context`.
#[derive(Debug)]
pub struct Offset(usize);

impl Offset {
    /// Creates a new `Offset` from index `0`.
    pub fn new() -> Offset {
        Offset(0)
    }

    /// Shifts the inner offset by the provided shift value.Reducer
    ///
    /// The newly shifted offset is then returned, for convenience.
    pub fn shift(&mut self, shift: usize) -> usize {
        self.0 += shift;
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_shifting() {
        let mut offset = Offset::new();

        let one = offset.shift(1);
        let two = offset.shift(1);
        let ten = offset.shift(8);

        assert_eq!(one, 1);
        assert_eq!(two, 2);
        assert_eq!(ten, 10);
    }
}
