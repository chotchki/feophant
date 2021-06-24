Welcome to Rusty Elephant! Chris's attempt to learn rust. As a toy I'm implementing a SQL database that can accept pgbench input.

[![Test with Source Code Coverage](https://github.com/chotchki/rusty-elephant/actions/workflows/test_source_coverage.yaml/badge.svg)](https://github.com/chotchki/rusty-elephant/actions/workflows/test_source_coverage.yaml) [![codecov](https://codecov.io/gh/chotchki/rusty-elephant/branch/main/graph/badge.svg?token=6JV9391LY0)](https://codecov.io/gh/chotchki/rusty-elephant)

Just a toy but it's already taught me a lot about Rust.

# Launch
Launch the server
`./rusty-elephant`

Lauch a postgres client application to test
`./pgbench -h 127.0.0.1 -p 50000`

# What works user facing
You can currently start the server, connect to it and have it throw tons of errors. To support more there is a ton of infrastructure required to wire up next steps.

# Current TODO List - Subject to constant change!
**Path to 0.4:** 
Implement insert to a system table such as pg_class.

* Renamed files and folders in the engine to correspond to how the postgres stages work.
* I still like the speciality raw types I made in ParseTree but as I understand more I might get rid of it.

Sample explain plan for insert, explain doesn't work for create table.
```
postgres=# explain insert into foo values ('temp');
                    QUERY PLAN                    
--------------------------------------------------
 Insert on foo  (cost=0.00..0.01 rows=1 width=32)
   ->  Result  (cost=0.00..0.01 rows=1 width=32)
(2 rows)
```

Pipeline: -(string)> sql_parser -(ParseTree)> analyzer -(QueryTree)> rewriter -(QueryTree)> planner -(PlannedStatement)> executer -> do stuff

Pipeline has been plumbed into client_processor. Now to make create table work since inserts need a non system table. (done)

Now let's do inserts!



**Path to 0.5**
Implement nullable columns, the underlying data structures already support it. Would move this up except that I don't have an easy way to test it.

**Path to 0.6**
Implement unique indexes. Inserts should fail on violations.

**Path to 0.7**
Implement single non joined select. No wildcards, aliases or other items.

**Path to 0.8**
At this point I have enough infrastructure to start caring about transactions. Implement filtering of tuples based on visibility rules.

**Path to 0.9**
Implement delete for tuples

**Path to 0.10:**
pgbench setup can run successfully, in memory

**Path to 0.11**
Did some reading on how the buffer manager works and my implementation seems to be firmly in the right direction. Take that knowledge and implement persistence

**1.0 Release Criteria**
pgbench can run successfully
Pick a new distinct name, rename everything
Pick a license
Setup fuzz testing
Persist to disk with moderate crash safety
Be prepared to actually use it


# # Longer Term TODO
This is stuff that I should get to but aren't vital to getting to a minimal viable product.
* Right now the main function runs the server from primitives. The Tokio Tower layer will probably do it better.
* The codec that parses the network traffic is pretty naive. You could make the server allocate 2GB of data for a DDOS easily.
* * We should either add state to the codec or change how it parses to produce chunked requests. That means that when the 2GB offer is reached the server can react and terminate before we accept too much data. Its a little more nuanced than that, 2GB input might be okay but we should make decisions based on users and roles.

# Postgres Divergance
Its kinda pointless to blindly reproduce what has already been done so I'm making the following changes to the db server design vs Postgres.

* Rust's memory safety and strong type system.
* Multi-threaded async design based on Tokio instead of Postgres's multi-process design.
* * Perk of this is not needing to manage SYSV shared memory. (Postgres largely fixed this but I think its still worth noting).
* Want to avoid vaccuum for transaction wrap around. Will try 64-bit transaction IDs but might go to 128-bit.
* * I can avoid the need to freeze Transaction IDs however the hint bits will need scanning to ensure that they are updated.
* Replacing OIDs with UUIDv4s.


## Rust Notes
How to setup modules sanely: https://dev.to/stevepryde/intro-to-rust-modules-3g8k

Reasonable application error type creation: https://github.com/dtolnay/anyhow

Library Errors: https://github.com/dtolnay/thiserror

Rust's inability to treat enum variants as a type is a HUGE pain. I cheated and separated serialization from deserialization.

# Legal Stuff (Note I'm not a lawyer!)
I am explicitly striving for SQL+Driver compatibility with [PostgreSQL](https://www.postgresql.org) so things such as system tables and code that handles them will be named the same. I don't think this violates their [trademark policy](https://www.postgresql.org/about/policies/trademarks/) but if I am please just reach out to me! I have also gone with a pretty restrictive license but I'm not tied to it if that is causing an issue for others who are using the code.