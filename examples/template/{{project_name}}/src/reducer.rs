//! `Reducer` implementation for the {{project_name}} project.
extern crate efflux;
use efflux::prelude::*;

{% set project_clean = project_name | slugify | title | split(pat="-") | join(sep="") -%}
{% set project_struct = project_clean ~ "Reducer" -%}

fn main() {
    // execute the reduction phase
    efflux::run_reducer({{ project_struct }});
}

/// The struct which will implement the `Reducer` trait.
struct {{ project_struct }};

/// An empty implementation of the `Reducer` trait.
impl Reducer for {{ project_struct }} {
    fn setup(&mut self, _ctx: &mut Context) {
        /// Carry out any setup required in this block.
    }

    fn reduce(&mut self, key: String, values: Vec<String>, ctx: &mut Context) {
        /// Carry out the main reducer tasks inside this block.
    }

    fn cleanup(&mut self, _ctx: &mut Context) {
        /// Carry out any cleanup required in this block.
    }
}
