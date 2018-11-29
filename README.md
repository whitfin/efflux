# Efflux
[![Crates.io](https://img.shields.io/crates/v/efflux.svg)](https://crates.io/crates/efflux) [![Build Status](https://img.shields.io/travis/whitfin/efflux.svg)](https://travis-ci.org/whitfin/efflux)

Efflux is a set of Rust interfaces for MapReduce and Hadoop Streaming. It enables Rust developers to run batch jobs on Hadoop infrastructure whilst staying with the efficiency and safety they're used to.

Initially written to scratch a personal itch, this crate offers simple traits to mask the internals of working with Hadoop Streaming which lend themselves well to writing jobs quickly. Functionality is handed off to macros where possible to provide compile time guarantees, and any other functionality is kept simple to avoid overhead wherever possible.

## Installation

Efflux is available on [crates.io](https://crates.io/crates/efflux) as a library crate, so you only need to add it as a dependency:

```toml
[dependencies]
efflux = "1.1"
```

You can then gain access to everything relevant using the `prelude` module of Efflux:

```rust
use efflux::prelude::*;
```

## Usage

Efflux comes with a handy template to help generate new projects, using the [kickstart](https://github.com/Keats/kickstart) tool. You can simply use the commands below and follow the prompt to generate a new project skeleton:

```shell
# install kickstart
$ cargo install kickstart

# create a project from the template
$ kickstart -s examples/template https://github.com/whitfin/efflux
```

If you'd rather not use the templating tool, you can always work from the examples found in this repository. A good place to start is the traditional [wordcount](examples/wordcount) example.

## Testing

Testing your binaries is actually fairly simple, as you can simulate the Hadoop phases using a basic UNIX pipeline. The following example replicates the Hadoop job flow and generates output that matches a job executed with Hadoop itself:

```shell
# example Hadoop task invocation
$ hadoop jar hadoop-streaming-2.8.2.jar \
    -input <INPUT> \
    -output <OUTPUT> \
    -mapper <MAPPER> \
    -reducer <REDUCER>

# example simulation run via UNIX utilities
$ cat <INPUT> | <MAPPER> | sort -k1,1 | <REDUCER> > <OUTPUT>
```

This can be tested using the [wordcount](examples/wordcount) example to confirm that the outputs are indeed the same. There may be some cases where output differs, but it should be sufficient for many cases.
