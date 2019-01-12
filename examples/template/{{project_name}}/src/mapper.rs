//! `Mapper` implementation for the {{project_name}} project.
extern crate efflux;
use efflux::prelude::*;

{% set project_clean = project_name | slugify | title | split(pat="-") | join(sep="") -%}
{% set project_struct = project_clean ~ "Mapper" -%}

fn main() {
    // execute the mapping phase
    efflux::run_mapper({{project_struct}});
}

/// The struct which will implement the `Mapper` trait.
struct {{project_struct}};

/// An empty implementation of the `Mapper` trait.
impl Mapper for {{project_struct}} {
    fn setup(&mut self, _ctx: &mut Context) {
        // Carry out any setup required in this block.
    }

    fn map(&mut self, _key: usize, _value: &[u8], _ctx: &mut Context) {
        // Carry out the main mapping tasks inside this block.
    }

    fn cleanup(&mut self, _ctx: &mut Context) {
        // Carry out any cleanup required in this block.
    }
}
