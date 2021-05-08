Welcome to Rusty Elephant! Chris's attempt to learn rust. As a toy I'm implementing a SQL database that can accept pgbench input.

Just a toy but it's already taught me a lot about Rust.

# Launch

Launch the server
`./rusty-elephant`

Lauch a postgres client application to test
`./pgbench -h 127.0.0.1 -p 50000`


# Architecture and structure (for the eighth time or more)

main.rs is the entry point it listens and on connection hands off process handling to a connection object