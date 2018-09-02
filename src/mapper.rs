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
    fn map(&mut self, key: usize, val: String, ctx: &mut Context) {
        ctx.write(key, val);
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
    }

    /// Passes each entry through to the mapper as a value, with the current
    /// byte offset being provided as the key (this follows the implementation
    /// provided in the Hadoop MapReduce Java interfaces, but it's unclear as
    /// to whether this is the desired default behaviour here).
    fn on_entry(&mut self, line: String, ctx: &mut Context) {
        let offset = {
            // grabs the offset from the context, and shifts the offset
            ctx.get_mut::<Offset>().unwrap().shift(line.len() + 2)
        };

        self.0.map(offset, line, ctx);
    }
}
