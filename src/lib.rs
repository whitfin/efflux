//! Efflux is a set of Rust interfaces for MapReduce and Hadoop Streaming.
//!
//! This crate provides easy interfaces for working with MapReduce, whether
//! or not you're running on the Hadoop platform. Usage is as simple as a
//! struct which implements either the `Mapper` or `Reducer` trait, as all
//! other interaction is taken care of internally.
//!
//! Macros are provided for IO, to provide a compile-time guarantee of things
//! such as counter/status updates, or writing to the Hadoop task logs.
#![doc(html_root_url = "https://docs.rs/efflux/1.2.0")]
#[macro_use]
pub mod macros;
pub mod context;
pub mod io;
pub mod mapper;
pub mod reducer;

use self::mapper::Mapper;
use self::reducer::Reducer;

use self::mapper::MapperLifecycle;
use self::reducer::ReducerLifecycle;

use self::io::run_lifecycle;

/// Executes a `Mapper` against the current `stdin`.
pub fn run_mapper<M>(mapper: M)
where
    M: Mapper + 'static,
{
    run_lifecycle(MapperLifecycle::new(mapper));
}

/// Executes a `Reducer` against the current `stdin`.
pub fn run_reducer<R>(reducer: R)
where
    R: Reducer + 'static,
{
    run_lifecycle(ReducerLifecycle::new(reducer));
}

// prelude module
pub mod prelude {
    //! A "prelude" for crates using the `efflux` crate.
    //!
    //! This prelude contains the required imports for almost all use cases, to
    //! avoid having to include modules and structures directly:
    //!
    //! ```rust
    //! use efflux::prelude::*;
    //! ```
    //!
    //! The prelude may grow over time, but it is unlikely to shrink.
    pub use super::context::{Configuration, Context, Contextual};
    pub use super::mapper::Mapper;
    pub use super::reducer::Reducer;
}
