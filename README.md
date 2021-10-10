# FeOphant

A SQL database server written in Rust and inspired by PostreSQL.

We now have support for persistent storage! Not crash safe but I'm getting there!

[![Latest Build][build-badge]][build-url]
[![codecov][codecov-badge]][codecov-url]
[![dependency status][dep-badge]][dep-url]

[build-badge]: https://github.com/chotchki/feophant/actions/workflows/test_source_coverage.yaml/badge.svg
[build-url]: https://github.com/chotchki/feophant/actions/workflows/test_source_coverage.yaml
[codecov-badge]: https://codecov.io/gh/chotchki/feophant/branch/main/graph/badge.svg?token=6JV9391LY0
[codecov-url]: https://codecov.io/gh/chotchki/feophant
[dep-badge]: https://deps.rs/repo/github/chotchki/feophant/status.svg
[dep-url]: https://deps.rs/repo/github/chotchki/feophant

[Website](https://feophant.com)

## Launch

Launch the server
`./feophant`

Lauch a postgres client application to test
`./pgbench -h 127.0.0.1 -p 50000`
`./psql -h 127.0.0.1 -p 50000`

Benchmark to aid in profiling
`cargo instruments --bench feophant_benchmark -t time`

## What works user facing
* Connecting unauthenticated using a postgres client/driver. 
* You can create tables, insert data and query single tables.
* Data is persisted to disk, not crash safe and the on disk format is NOT stable.

## Postgres Divergance

Its kinda pointless to blindly reproduce what has already been done so I'm making the following changes to the db server design vs Postgres.

* Rust's memory safety and strong type system.
* Multi-threaded async design based on Tokio instead of Postgres's multi-process design.
* * Perk of this is not needing to manage SYSV shared memory. (Postgres largely fixed this but I think its still worth noting).
* Want to avoid vaccuum for transaction wrap around. Will try 64-bit transaction IDs but might go to 128-bit.
* * I can avoid the need to freeze Transaction IDs however the hint bits will need scanning to ensure that they are updated.
* Replacing OIDs with UUIDv4s.

* I think I've figured out what the core divergeance from Postgres that I'm interested in. I love Postgres's transactional DDLs but version controlling a schema is awful. What if I make the database server a library and your schema is code? You supply a new binary that runs as the database server and if you need to change it you just deploy the binary instead? Then the compiler can optimize out anything you don't need to run the system in your use case. The hardest part is dealing with schema changes that affect your on disk format.


### Rust Notes

How to setup modules sanely: https://dev.to/stevepryde/intro-to-rust-modules-3g8k

Reasonable application error type creation: https://github.com/dtolnay/anyhow

Library Errors: https://github.com/dtolnay/thiserror

Rust's inability to treat enum variants as a type is a HUGE pain. I end up having an enum to hold data and another enum to validate and match the sub type. The [RFC](https://github.com/rust-lang/rfcs/pull/2593) to fix this was postponed indefinately.


## Legal Stuff (Note I'm not a lawyer!)

I am explicitly striving for SQL+Driver compatibility with [PostgreSQL](https://www.postgresql.org) so things such as system tables and code that handles them will be named the same. I don't think this violates their [trademark policy](https://www.postgresql.org/about/policies/trademarks/) but if I am please just reach out to me! I have also gone with a pretty restrictive license but I'm not tied to it if that is causing an issue for others who are using the code.