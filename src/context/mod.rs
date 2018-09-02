//! Hadoop job context representations and bindings.
//!
//! This module exposes an arbitrarily typed map to be used as a job context
//! for all Hadoop stages. It can be used to lookup different types and store
//! state across executions of a task (although note that it's local to each
//! mapper/reduce process).
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
//! - `Group`
//! - `Offset`
//!
//! The most interesting of these types is the `Configuration` type, as it
//! represents the job configuration provided by Hadoop.
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Display;

mod conf;
mod delim;
mod group;
mod offset;

pub use self::conf::Configuration;
pub use self::delim::Delimiters;
pub use self::group::Group;
pub use self::offset::Offset;

/// Marker trait to represent types which can be added to a `Context`.
pub trait Contextual: Any {}

// all internal contextual types
impl Contextual for Configuration {}
impl Contextual for Delimiters {}
impl Contextual for Group {}
impl Contextual for Offset {}

/// Context structure to represent a Hadoop job context.
///
/// This acts as an arbitrarily-typed bag, allowing for easy storage
/// of random types between iterations of the stage. See the module
/// documentation for further details and examples.
#[derive(Debug)]
pub struct Context {
    data: HashMap<TypeId, Box<Any>>,
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
    pub fn write<K, V>(&mut self, key: K, val: V)
    where
        K: Display,
        V: Display,
    {
        let out = self.get::<Delimiters>().unwrap().output();
        println!("{}{}{}", key, out, val);
    }
}
