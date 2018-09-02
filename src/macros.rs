//! Compile time utilities to ease Hadoop usage.

/// Prints output to the Hadoop task logs.
///
/// As `::std::io::stdout` is used to Hadoop Streaming writes, logging
/// must go through this macro instead to successfully make it to the logs.
#[macro_export]
macro_rules! log {
    () => (eprintln!());
    ($fmt:expr) => (eprintln!($fmt));
    ($fmt:expr, $($arg:tt)*) => (eprintln!($fmt, $($arg)*));
}

/// Updates a counter for the current job.
///
/// A counter belongs to a group by a label; as such both must be given
/// to this macro in order to compile correctly. Note that neither the
/// group nor label can contain a `","`, as Hadoop uses this to split
/// the IO stream.
///
/// This is simply a sane wrapper around `log!` to ensure that
/// counter updates are always logged in the correct formatting.
#[macro_export]
macro_rules! update_counter {
    ($group:expr, $label:expr, $amount:expr) => {
        log!("reporter:counter:{},{},{}", $group, $label, $amount);
    };
}

/// Updates the status for the current job.
///
/// This is simply a sane wrapper around `log!` to ensure that
/// status updates are always logged in the correct formatting.
#[macro_export]
macro_rules! update_status {
    ($status:expr) => {
        log!("reporter:status:{}", $status);
    };
}
