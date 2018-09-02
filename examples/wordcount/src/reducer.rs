//! Reducer binary for the MapReduce word counter example.
extern crate efflux;

use efflux::prelude::{Context, Reducer};

fn main() {
    // simply run the reduction phase with our reducer
    efflux::run_reducer(WordcountReducer);
}

/// Simple struct to represent a word counter reducer.
struct WordcountReducer;

// Reducing stage implementation.
impl Reducer for WordcountReducer {
    /// Reduction implementation for the word counter example.
    fn reduce(&mut self, key: String, values: Vec<String>, ctx: &mut Context) {
        // base counter
        let mut count = 0;

        for value in values {
            // sum all values to obtain total appearances
            count += value.parse::<usize>().unwrap();
        }

        // write the word and the total count
        ctx.write(key, count);
    }
}
