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

## Rust Notes
How to setup modules sanely: https://dev.to/stevepryde/intro-to-rust-modules-3g8k

Reasonable application error type creation: https://github.com/dtolnay/anyhow
Library Errors: https://github.com/dtolnay/thiserror