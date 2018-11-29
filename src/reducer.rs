//! Exposed structures based on the reduction stage.
//!
//! This module offers the `Reducer` trait, which allows a developer
//! to easily create a reduction stage due to the sane defaults. Also
//! offered is the `ReducerLifecycle` binding for use as an IO stage.
use context::{Context, Delimiters, Group};
use io::Lifecycle;

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
    fn reduce(&mut self, key: String, values: Vec<String>, ctx: &mut Context) {
        for value in values {
            ctx.write(&key, value);
        }
    }

    /// Cleanup handler for the current `Reducer`.
    fn cleanup(&mut self, _ctx: &mut Context) {}
}

/// Enables raw functions to act as `Reducer` types.
impl<R> Reducer for R
where
    R: FnMut(String, Vec<String>, &mut Context),
{
    /// Reduction handler by passing through the values to the inner closure.
    fn reduce(&mut self, key: String, value: Vec<String>, ctx: &mut Context) {
        self(key, value, ctx)
    }
}

/// Lifecycle structure to represent a reduction.
pub struct ReducerLifecycle<R>(pub R)
where
    R: Reducer;

/// `Lifecycle` implementation for the reduction stage.
impl<R> Lifecycle for ReducerLifecycle<R>
where
    R: Reducer,
{
    /// Creates all required state for the lifecycle.
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.insert(Group::new());
        self.0.setup(ctx);
    }

    /// Processes each entry by buffering sequential key entries into the
    /// internal group. Once the key changes the prior group is passed off
    /// into the actual `Reducer` trait, and the group is reset.
    fn on_entry(&mut self, input: String, ctx: &mut Context) {
        let (key, value) = {
            // grab the delimiters from the context
            let delim = ctx.get::<Delimiters>().unwrap();

            // split on the input delimiter
            let mut split = input.splitn(2, delim.input());

            // grab key/value, default to empty string
            let key = split.next().unwrap_or("").to_owned();
            let val = split.next().unwrap_or("").to_owned();

            (key, val)
        };

        let (key, values) = {
            // borrow a mutable group from the context
            let group = ctx.get_mut::<Group>().unwrap();

            // first key given
            if group.is_unset() {
                group.reset(&key);
            }

            // append to buffer
            if group.key() == key {
                group.push(value);
                return;
            }

            // new key, reset group
            let block = group.reset(&key);
            group.push(value);
            block
        };

        // reduce the key and value group
        self.0.reduce(key, values, ctx);
    }

    /// Finalizes the lifecycle by emitting any leftover pairs.
    fn on_end(&mut self, ctx: &mut Context) {
        // grab the group and reset to a blank key
        let (key, values) = { ctx.get_mut::<Group>().unwrap().reset("") };

        // reduce the last batches
        self.0.reduce(key, values, ctx);
        self.0.cleanup(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::Contextual;
    use io::Lifecycle;

    #[test]
    fn test_reducer_lifecycle() {
        let mut ctx = Context::new();
        let mut reducer = ReducerLifecycle(TestReducer);

        reducer.on_start(&mut ctx);

        {
            reducer.on_entry("first\tone".into(), &mut ctx);
            reducer.on_entry("first\ttwo".into(), &mut ctx);
            reducer.on_entry("first\tthree".into(), &mut ctx);
            reducer.on_entry("second\tone".into(), &mut ctx);
            reducer.on_entry("second\ttwo".into(), &mut ctx);
            reducer.on_entry("second\tthree".into(), &mut ctx);

            let pair = ctx.get::<TestPair>();

            assert!(pair.is_some());

            let pair = pair.unwrap();

            assert_eq!(pair.0, "first");
            assert_eq!(pair.1, vec!["one", "two", "three"]);
        }

        reducer.on_end(&mut ctx);

        let pair = ctx.get::<TestPair>();

        assert!(pair.is_some());

        let pair = pair.unwrap();

        assert_eq!(pair.0, "second");
        assert_eq!(pair.1, vec!["one", "two", "three"]);
    }

    struct TestPair(String, Vec<String>);
    struct TestReducer;

    impl Contextual for TestPair {}

    impl Reducer for TestReducer {
        fn reduce(&mut self, key: String, values: Vec<String>, ctx: &mut Context) {
            ctx.insert(TestPair(key, values));
        }
    }
}
