# Efflux
[![Crates.io](https://img.shields.io/crates/v/efflux.svg)](https://crates.io/crates/efflux) [![Unix Build Status](https://img.shields.io/travis/whitfin/efflux.svg?label=unix)](https://travis-ci.org/whitfin/efflux) [![Windows Build Status](https://img.shields.io/appveyor/ci/whitfin/efflux.svg?label=win)](https://ci.appveyor.com/project/whitfin/efflux)

Efflux is a set of Rust interface for MapReduce and Hadoop Streaming. It enables Rust developers to run batch jobs on Hadoop infrastructure whilst staying with the efficient and safety they're used to.

Initially written to scratch a personal itch, this crate offers simple traits to mask the internals of working with Hadoop Streaming which lend themselves well to writing jobs quickly. Functionality is handed off to macros where possible to provide compile time guarantees, and any other functionality is kept simple to avoid overhead wherever possible.

## Installation

Efflux is available on [crates.io](https://crates.io/crates/efflux) as a library crate, so you only need to add it as a dependency:

```toml
[dependencies]
efflux = "1.0.0"
```

You can then gain access to everything relevant using the `prelude` module of Efflux:

```rust
use efflux::prelude::*;
```

## Usage

Coming soon, as things are currently in _flux_.
