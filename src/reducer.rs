//! Exposed structures based on the reduction stage.
//!
//! This module offers the `Reducer` trait, which allows a developer
//! to easily create a reduction stage due to the sane defaults. Also
//! offered is the `ReducerLifecycle` binding for use as an IO stage.
use crate::context::{Context, Delimiters};
use crate::io::Lifecycle;

/// Trait to represent the reduction stage of MapReduce.
///
/// All trait methods have sane defaults to match the Hadoop MapReduce
/// implementation, allowing the developer to pick and choose what they
/// customize without having to write a large amount of boilerplate.
pub trait Reducer {
    /// Setup handler for the current `Reducer`.
    fn setup(&mut self, _ctx: &mut Context) {}

    /// Reduction handler for the current `Reducer`.
    ///
    /// The default implementation of this handler will emit each value against
    /// the key in the order they were received. This is typically the stage of
    /// interest for many MapReduce developers.
    fn reduce(&mut self, key: &[u8], values: &[&[u8]], ctx: &mut Context) {
        for value in values {
            ctx.write(key, value);
        }
    }

    /// Cleanup handler for the current `Reducer`.
    fn cleanup(&mut self, _ctx: &mut Context) {}
}

/// Enables raw functions to act as `Reducer` types.
impl<R> Reducer for R
where
    R: FnMut(&[u8], &[&[u8]], &mut Context),
{
    /// Reduction handler by passing through the values to the inner closure.
    fn reduce(&mut self, key: &[u8], value: &[&[u8]], ctx: &mut Context) {
        self(key, value, ctx)
    }
}

/// Lifecycle structure to represent a reduction.
pub(crate) struct ReducerLifecycle<R>
where
    R: Reducer,
{
    on: bool,
    key: Vec<u8>,
    values: Vec<Vec<u8>>,
    reducer: R,
}

/// Basic creation for `ReducerLifecycle`
impl<R> ReducerLifecycle<R>
where
    R: Reducer,
{
    /// Constructs a new `ReducerLifecycle` instance.
    pub(crate) fn new(reducer: R) -> Self {
        Self {
            reducer,
            on: false,
            key: Vec::new(),
            values: Vec::new(),
        }
    }
}

/// `Lifecycle` implementation for the reduction stage.
impl<R> Lifecycle for ReducerLifecycle<R>
where
    R: Reducer,
{
    /// Creates all required state for the lifecycle.
    fn on_start(&mut self, ctx: &mut Context) {
        self.reducer.setup(ctx);
    }

    /// Processes each entry by buffering sequential key entries into the
    /// internal group. Once the key changes the prior group is passed off
    /// into the actual `Reducer` trait, and the group is reset.
    fn on_entry(&mut self, input: &[u8], ctx: &mut Context) {
        let (key, value) = {
            // grab the delimiters from the context
            let delim = ctx.get::<Delimiters>().unwrap();

            // search (quickly) for the input byte delimiter
            match twoway::find_bytes(&input, delim.input()) {
                Some(n) if n < input.len() => {
                    // split the input at the given index when applicable
                    (&input[..n], &input[n + delim.input().len()..])
                }

                // otherwise the input is the key
                _ => (&input[..], &b""[..]),
            }
        };

        // first key
        if !self.on {
            self.on = true;
            self.key.clear();
            self.key.extend(key);
        }

        // append to buffer
        if self.key == key {
            self.values.push(value.to_vec());
            return;
        }

        // construct a references list to avoid exposing vecs
        let mut values = Vec::with_capacity(self.values.len());
        for value in &self.values {
            values.push(value.as_slice());
        }

        // reduce the key and value group
        self.reducer.reduce(&self.key, &values, ctx);

        // reset the key
        self.key.clear();
        self.key.extend(key);

        // drain the internal buffer
        self.values.clear();
        self.values.push(value.to_vec());
    }

    /// Finalizes the lifecycle by emitting any leftover pairs.
    fn on_end(&mut self, ctx: &mut Context) {
        // construct a references list to avoid exposing vecs
        let mut values = Vec::with_capacity(self.values.len());
        for value in &self.values {
            values.push(value.as_slice());
        }

        // reduce the last batche of values
        self.reducer.reduce(&self.key, &values, ctx);
        self.reducer.cleanup(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Contextual;
    use crate::io::Lifecycle;

    #[test]
    fn test_reducer_lifecycle() {
        let mut ctx = Context::new();
        let mut reducer = ReducerLifecycle::new(TestReducer);

        reducer.on_start(&mut ctx);

        {
            reducer.on_entry(b"first\tone", &mut ctx);
            reducer.on_entry(b"first\ttwo", &mut ctx);
            reducer.on_entry(b"first\tthree", &mut ctx);
            reducer.on_entry(b"second\tone", &mut ctx);
            reducer.on_entry(b"second\ttwo", &mut ctx);
            reducer.on_entry(b"second\tthree", &mut ctx);

            let pair = ctx.get::<TestPair>();

            assert!(pair.is_some());

            let pair = pair.unwrap();

            assert_eq!(pair.0, b"first");
            assert_eq!(pair.1, vec![&b"one"[..], b"two", b"three"]);
        }

        reducer.on_end(&mut ctx);

        let pair = ctx.get::<TestPair>();

        assert!(pair.is_some());

        let pair = pair.unwrap();

        assert_eq!(pair.0, b"second");
        assert_eq!(pair.1, vec![&b"one"[..], b"two", b"three"]);
    }

    #[test]
    fn test_reducer_empty_values() {
        let mut ctx = Context::new();
        let mut reducer = ReducerLifecycle::new(TestReducer);

        reducer.on_start(&mut ctx);
        reducer.on_entry(b"key", &mut ctx);
        reducer.on_entry(b"key\t", &mut ctx);
        reducer.on_end(&mut ctx);

        let pair = ctx.get::<TestPair>();

        assert!(pair.is_some());

        let pair = pair.unwrap();

        assert_eq!(pair.0, b"key");
        assert_eq!(pair.1, vec![b"", b""]);
    }

    struct TestPair(Vec<u8>, Vec<Vec<u8>>);
    struct TestReducer;

    impl Contextual for TestPair {}

    impl Reducer for TestReducer {
        fn reduce(&mut self, key: &[u8], values: &[&[u8]], ctx: &mut Context) {
            let mut stored = Vec::new();
            for value in values {
                stored.push(value.to_vec());
            }
            ctx.insert(TestPair(key.to_vec(), stored));
        }
    }
}
