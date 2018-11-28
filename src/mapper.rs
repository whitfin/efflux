//! Exposed structures based on the mapping stage.
//!
//! This module offers the `Mapper` trait, which allows a developer
//! to easily create a mapping stage due to the sane defaults. Also
//! offered is the `MapperLifecycle` binding for use as an IO stage.
use context::{Context, Offset};
use io::Lifecycle;

/// Trait to represent the mapping stage of MapReduce.
///
/// All trait methods have sane defaults to match the Hadoop MapReduce
/// implementation, allowing the developer to pick and choose what they
/// customize without having to write a large amount of boilerplate.
pub trait Mapper {
    /// Setup handler for the current `Mapper`.
    fn setup(&mut self, _ctx: &mut Context) {}

    /// Mapping handler for the current `Mapper`.
    ///
    /// The default implementation is to simply emit each key/value pair as they
    /// are received, without any changes. As such, this is where most developers
    /// will immediately begin to change things.
    fn map(&mut self, key: usize, value: String, ctx: &mut Context) {
        ctx.write(key, value);
    }

    /// Cleanup handler for the current `Mapper`.
    fn cleanup(&mut self, _ctx: &mut Context) {}
}

/// Lifecycle structure to represent a mapping.
pub struct MapperLifecycle<M>(pub M)
where
    M: Mapper;

/// `Lifecycle` implementation for the mapping stage.
impl<M> Lifecycle for MapperLifecycle<M>
where
    M: Mapper,
{
    /// Creates all required state for the lifecycle.
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.insert(Offset::new());
        self.0.setup(ctx);
    }

    /// Passes each entry through to the mapper as a value, with the current
    /// byte offset being provided as the key (this follows the implementation
    /// provided in the Hadoop MapReduce Java interfaces, but it's unclear as
    /// to whether this is the desired default behaviour here).
    fn on_entry(&mut self, input: String, ctx: &mut Context) {
        let offset = {
            // grabs the offset from the context, and shifts the offset
            ctx.get_mut::<Offset>().unwrap().shift(input.len() + 2)
        };

        self.0.map(offset, input, ctx);
    }

    /// Finalizes the lifecycle by calling cleanup.
    fn on_end(&mut self, ctx: &mut Context) {
        self.0.cleanup(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use context::Contextual;
    use io::Lifecycle;

    #[test]
    fn test_mapper_lifecycle() {
        let mut ctx = Context::new();
        let mut mapper = MapperLifecycle(TestMapper);

        mapper.on_start(&mut ctx);

        {
            let mut vet = |input: &str, expected: usize| {
                mapper.on_entry(input.into(), &mut ctx);

                let pair = ctx.get::<TestPair>();

                assert!(pair.is_some());

                let pair = pair.unwrap();

                assert_eq!(pair.0, expected);
                assert_eq!(pair.1, input);
            };

            vet("first_input_line", 18);
            vet("second_input_line", 37);
            vet("third_input_line", 55);
        }

        mapper.on_end(&mut ctx);
    }

    struct TestPair(usize, String);

    impl Contextual for TestPair {}

    struct TestMapper;

    impl Mapper for TestMapper {
        fn map(&mut self, key: usize, val: String, ctx: &mut Context) {
            ctx.insert(TestPair(key, val));
        }
    }
}
