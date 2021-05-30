Welcome to Rusty Elephant! Chris's attempt to learn rust. As a toy I'm implementing a SQL database that can accept pgbench input.

[![Rust](https://github.com/chotchki/rusty-elephant/actions/workflows/rust.yml/badge.svg)](https://github.com/chotchki/rusty-elephant/actions/workflows/rust.yml)

Just a toy but it's already taught me a lot about Rust.

# Launch

Launch the server
`./rusty-elephant`

Lauch a postgres client application to test
`./pgbench -h 127.0.0.1 -p 50000`


# What works

You can currently start the server, connect to it and have it throw tons of errors. I'm to the point now I need to start supporting saving data.

# Next TODO

Need to support the concept of a table that can be read and written to, in memory.

Memory based pages are complete now need a concept of taking a table definition (hardcoded pg_class first) and storing an entry in a page. This has morphed into figuring out the type system, Argh!

What I need:
* To support various data types for use on columns/rows.
* A way to serialize and deserialize them to bytes (have that for text+uuid)
* A way to say to a function with this list of types (which may be different), parse this data into something sane.
* * In rust this implies an enum OR a trait.
* * * I have implemented both of these and have fallen on my face due to the de-serialization process.
* * * Not sure if I was being dumb but I thought it would be reasonable to have a type know how to deserialize itself.
* * * This breaks for enums and traits in frustrating ways.
* * * * Enums variants can't be matched on which makes this REALLY hard.


# Postgres Divergance

Its kinda pointless to blindly reproduce what has already been done so I'm making the following changes to the db server design vs Postgres.

* Multi-threaded design based on Tokio instead of Postgres's multi-process design.
* Want to avoid vaccuum for transaction wrap around. Will try 64-bit transaction IDs but might go to 128-bit.
* Replacing OIDs with UUIDv4s.

## Rust Notes
How to setup modules sanely: https://dev.to/stevepryde/intro-to-rust-modules-3g8k

Reasonable application error type creation: https://github.com/dtolnay/anyhow
Library Errors: https://github.com/dtolnay/thiserror

Rust's inability to treat enum variants as a type is a HUGE pain. Essentially I might have to revert to fighting traits instead 😤.

# Legal Stuff (Note I'm not a lawyer!)
I am explicitly striving for SQL+Driver compatibility with [PostgreSQL|https://www.postgresql.org] so things such as system tables and code that handles them will be named the same. I don't think this violates their [trademark policy|https://www.postgresql.org/about/policies/trademarks/] but if I am please just reach out to me! I have also gone with a pretty restrictive license but I'm not tied to it if that is causing an issue for others who are using the code.