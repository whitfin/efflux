//! Mapper binary for the MapReduce word counter example.
extern crate efflux;
extern crate regex;

use efflux::prelude::{Context, Mapper};
use regex::Regex;

fn main() {
    // simply run the mapping phase with our mapper
    efflux::run_mapper(WordcountMapper::new());
}

/// Simple struct to represent a word counter mapper.
///
/// Contains several internal patterns to use when processing
/// the input text, to avoid re-compilation of Regex.
struct WordcountMapper {
    multi_spaces: Regex,
    punc_matcher: Regex,
}

impl WordcountMapper {
    /// Creates a new `WordcountMapper` with pre-compiled `Regex`.
    pub fn new() -> Self {
        Self {
            // detects multiple spaces in a row
            multi_spaces: Regex::new(r"\s{2,}").unwrap(),
            // detects punctuation followed by a space, or trailing
            punc_matcher: Regex::new(r"[[:punct:]](\s|$)").unwrap(),
        }
    }
}

// Mapping stage implementation.
impl Mapper for WordcountMapper {
    /// Mapping implementation for the word counter example.
    ///
    /// The input value is split into words using the internal patterns,
    /// and each word is then written to the context.
    fn map(&mut self, _key: usize, value: &[u8], ctx: &mut Context) {
        // skip empty
        if value.is_empty() {
            return;
        }

        // parse into a string using the input bytes
        let value = std::str::from_utf8(value).unwrap();

        // trim whitespaces
        let value = &value.trim();

        // remove all punctuation breaks (e.g. ". ")
        let value = self.punc_matcher.replace_all(&value, "$1");

        // compress all sequential spaces into a single space
        let value = self.multi_spaces.replace_all(&value, " ");

        // split on spaces to find words
        for word in value.split(" ") {
            // write each word
            ctx.write_fmt(word, 1);
        }
    }
}
