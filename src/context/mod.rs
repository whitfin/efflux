//! Hadoop job context representations and bindings.
//!
//! This module exposes an arbitrarily typed map to be used as a job context
//! for all Hadoop stages. It can be used to lookup different types and store
//! state across executions of a task (although note that it's local to each
//! mapper/reduce process). Authors of `Mapper` and `Reducer` implementations
//! shouldn't need to store state here as they have mutable access to their
//! struct values.
//!
//! Values can be references as `mut` when required, as there should be only
//! as single thread owning a `Context` at any given time. An example of
//! inserting a value and retrieving it is as follows:
//!
//! ```rust
//! # extern crate efflux;
//! use efflux::prelude::*;
//!
//! // custom state
//! #[derive(Eq, PartialEq)]
//! struct MyState {
//!     inner: usize
//! }
//!
//! // only `Contextual` structs can be store
//! impl Contextual for MyState {}
//!
//! // create a new context
//! let mut ctx = Context::new();
//!
//! // create the state
//! let state = MyState { inner: 3 };
//!
//! // store in context
//! ctx.insert(state);
//!
//! // get a reference back out, as an option
//! let state_ref = ctx.get::<MyState>().expect("state not found");
//!
//! // check it's the same state
//! assert_eq!(state_ref.inner, 3)
//! ```
//!
//! There are several types which will exist on a `Context` at various times
//! throughout execution due to internal use. Whilst these can be read by the
//! developer, they should rarely ever be modified as things may break. The
//! current set of `Contextual` types added are as follows:
//!
//! - `Configuration`
//! - `Delimiters`
//! - `Offset`
//!
//! The most interesting of these types is the `Configuration` type, as it
//! represents the job configuration provided by Hadoop.
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, Write};

mod conf;
mod delim;
mod offset;

pub use self::conf::Configuration;
pub use self::delim::Delimiters;
pub use self::offset::Offset;

/// Marker trait to represent types which can be added to a `Context`.
pub trait Contextual: Any {}

// all internal contextual types
impl Contextual for Configuration {}
impl Contextual for Delimiters {}
impl Contextual for Offset {}

/// Context structure to represent a Hadoop job context.
///
/// This acts as an arbitrarily-typed bag, allowing for easy storage
/// of random types between iterations of the stage. See the module
/// documentation for further details and examples.
#[derive(Debug, Default)]
pub struct Context {
    data: HashMap<TypeId, Box<dyn Any>>,
}

impl Context {
    /// Creates a new `Context`.
    pub fn new() -> Self {
        // new base container
        let mut ctx = Self {
            data: HashMap::new(),
        };

        // construct default types
        let conf = Configuration::new();
        let delim = Delimiters::new(&conf);

        // add both
        ctx.insert(conf);
        ctx.insert(delim);

        ctx
    }

    /// Retrieves a potential reference to a `Contextual` type.
    pub fn get<T: Contextual>(&self) -> Option<&T>
    where
        T: Contextual,
    {
        let types = TypeId::of::<T>();
        self.data.get(&types).and_then(|b| b.downcast_ref::<T>())
    }

    /// Retrieves a potential mutable reference to a `Contextual` type.
    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Contextual,
    {
        let types = TypeId::of::<T>();
        self.data
            .get_mut(&types)
            .and_then(|b| b.downcast_mut::<T>())
    }

    /// Inserts a `Contextual` type into the context.
    pub fn insert<T>(&mut self, t: T)
    where
        T: Contextual,
    {
        let types = TypeId::of::<T>();
        self.data.insert(types, Box::new(t));
    }

    /// Takes a `Contextual` type from the context.
    pub fn take<T>(&mut self) -> Option<T>
    where
        T: Contextual,
    {
        let types = TypeId::of::<T>();
        self.data
            .remove(&types)
            .and_then(|b| b.downcast::<T>().ok())
            .map(|t| *t)
    }

    /// Writes a key/value pair to the stage output.
    #[inline]
    pub fn write(&mut self, key: &[u8], val: &[u8]) {
        // grab a reference to the context output delimiters
        let out = self.get::<Delimiters>().unwrap().output();

        // lock the stdout buffer
        let stdout = io::stdout();
        let mut lock = stdout.lock();

        // write the pair and newline
        lock.write_all(key).unwrap();
        lock.write_all(out).unwrap();
        lock.write_all(val).unwrap();
        lock.write_all(b"\n").unwrap();
    }

    /// Writes a key/value formatted pair to the stage output.
    ///
    /// This is a simple sugar API around `write` which allows callers to
    /// provide a type which implements `Display` to serialize automatically.
    #[inline]
    pub fn write_fmt<K, V>(&mut self, key: K, val: V)
    where
        K: Display,
        V: Display,
    {
        self.write(key.to_string().as_bytes(), val.to_string().as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let ctx = Context::new();

        assert!(ctx.get::<Configuration>().is_some());
        assert!(ctx.get::<Delimiters>().is_some());
    }

    #[test]
    fn test_context_insertion() {
        let mut ctx = Context::new();
        let val = TestStruct(0);

        ctx.insert(val);

        assert!(ctx.get::<TestStruct>().is_some());
    }

    #[test]
    fn test_mutable_references() {
        let mut ctx = Context::new();
        let val = TestStruct(0);

        ctx.insert(val);

        {
            let mref = ctx.get_mut::<TestStruct>();
            assert!(mref.is_some());
            mref.unwrap().0 = 1;
        }

        let iref = ctx.get::<TestStruct>();

        assert!(iref.is_some());
        assert_eq!(iref.unwrap().0, 1);
    }

    #[test]
    fn test_taking_values() {
        let mut ctx = Context::new();
        let val = TestStruct(0);

        ctx.insert(val);

        let take = ctx.take::<TestStruct>();
        assert!(take.is_some());

        let take = ctx.take::<TestStruct>();
        assert!(take.is_none());
    }

    struct TestStruct(usize);
    impl Contextual for TestStruct {}
}
