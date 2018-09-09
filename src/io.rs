//! IO binding module for the `efflux` crate.
//!
//! Provides lifecycles for Hadoop Streaming IO, to allow the rest
//! of this crate to be a little more ignorant of how inputs flow.
use context::Context;
use std::io::{self, BufRead, BufReader};

/// Lifecycle trait to allow hooking into IO streams.
///
/// This will be implemented by all stages of MapReduce (e.g. to
/// appropriately handle buffering for the reduction stage). All
/// trait methods default to noop, as they're all optional.
pub trait Lifecycle {
    /// Startup hook for the IO stream.
    fn on_start(&mut self, _ctx: &mut Context) {}

    /// Entry hook for the IO stream to handle input values.
    fn on_entry(&mut self, _line: String, _ctx: &mut Context) {}

    /// Finalization hook for the IO stream.
    fn on_end(&mut self, _ctx: &mut Context) {}
}

/// Executes an IO `Lifecycle` against `io::stdin`.
pub fn run_lifecycle<L>(mut lifecycle: L)
where
    L: Lifecycle,
{
    // lock stdin for perf
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();

    // create a job context
    let mut ctx = Context::new();

    // fire the startup hooks
    lifecycle.on_start(&mut ctx);

    // read all inputs, and fire the entry hooks
    for line in BufReader::new(stdin_lock).lines() {
        if let Ok(line) = line {
            lifecycle.on_entry(line, &mut ctx);
        }
    }

    // fire the finalization hooks
    lifecycle.on_end(&mut ctx);
}
