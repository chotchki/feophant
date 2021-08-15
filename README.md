# FeOphant

A SQL database server written in Rust and inspired by PostreSQL.

We now have support for persistent storage! Not crash safe but I'm getting there!

[![Latest Build][build-badge]][build-url]
[![codecov][codecov-badge]][codecov-url]

[build-badge]: https://github.com/chotchki/feophant/actions/workflows/test_source_coverage.yaml/badge.svg
[build-url]: https://github.com/chotchki/feophant/actions/workflows/test_source_coverage.yaml
[codecov-badge]: https://codecov.io/gh/chotchki/feophant/branch/main/graph/badge.svg?token=6JV9391LY0
[codecov-url]: https://codecov.io/gh/chotchki/feophant

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

## Current TODO List - Subject to constant change!

**TODO**
Implement page level locks that are ordered to avoid deadlocking.

Acceptance Criteria:
* Should be able to update a row either inside a page or not without loosing commits.
* This is independent of transaction control so I think this sits below/in row manager.

**TODO**

Add support for defining a primary key on a table. This implies the following functionality:
* Index support through the stack down to the page level.
* The concept of unique indexes.
* Transactional support for indexes.
* Failure of a statement on constraint violation. Unsure if I'll end up with a general constraint system from this.

Based on reading this really means implementing Btree indexes. They don't seem to be that bad to understand/implement.

First and most important question, how should the index layers work?
    Are they transactional? (I don't think so until I implement a visability map)
    How should the low level layer function? 
        Should I have an Index config struct I pass around or just a table + columns + unique or not + type
        Index Config it is

Index Manager -> for a given table
IO Manager -> Handle Page Load / Store / Update

Implemented the formats but I think I need to add locking to the I/O manager.
    At a minimum I need to support a get for update, update and release lock.
    I'm not sure I understand how this should work :(. I think need to commit to another layer.

Back to indexes for now. I need to make a decision on how to handle them hitting the file system.
    Postgres uses a series of OIDs to map onto disk.

    I've been using uuids, I think I'm going to continue that. That would also solve the postgres fork approach.

Next up implementing the index manager to add entries to the index.

I'm having a hard time figuring this out, I might work to do the operations on the tree before I keep messing with the serialization protocols. I'm just worries they are directly linked.

Got further into the index manager. Unfortunately I need a lock manager to let it even pass the smell test. Time to go on a wild goose chase again! (This project is great for someone with ADHD to have fun on!)

The lock manager design/code is done but I'm not happy with using a rwlock to protect a tag. I really want to have the lock protect the content but that needs a way for me to support writeback. I think I need to build out two more things, a WAL mechanism and a buffer manager.

I guess I need to commit to doing this for reals. However I am worried about reaching a point of partially working for a while like when I did the type fixing. We'll see how this goes.

For now, the index implementation is now on hold until I get an integrated I/O subsystem and a stubbed out WAL.

**TODO**


Implement where clauses, will likely need to have to start tracing columns from analyizing through to later stages.


**TODO**

Implement support for running a fuzzer against the code base to ensure we are keeping the code at a high quality.

**TODO**

Implement delete for tuples

**TODO**
Implement the beginning parts of a WAL so that I can get to crash safety.

**TODO**
Defer parsing rows off disk until they are actually needed. I feel like I parse too early however any work on this should wait until I can really profile this.

**TODO**

pgbench setup can run successfully

**TODO**
Implement support for parameterized queries.

**TODO**

Ensure data about table structures is thread safe in the face of excessive Arc usage.

See where I can pass read only data by reference instead of uisng Arc everywhere

**TODO**

Support a row with more than 4kb of text in it.

**TODO**

Implement sorting.

**TODO**

Implement column aliasing

**TODO**

Implement subselect.

**TODO**

Implement Updates.

**1.0 Release Criteria**

* pgbench can run successfully
* ~~Pick a new distinct name, rename everything~~ Done
* Pick a license
* Setup fuzz testing
* Persist to disk with moderate crash safety
* Be prepared to actually use it


### Longer Term TODO

This is stuff that I should get to but aren't vital to getting to a minimal viable product.
* Right now the main function runs the server from primitives. The Tokio Tower layer will probably do it better.
* The codec that parses the network traffic is pretty naive. You could make the server allocate 2GB of data for a DDOS easily.
* * We should either add state to the codec or change how it parses to produce chunked requests. That means that when the 2GB offer is reached the server can react and terminate before we accept too much data. Its a little more nuanced than that, 2GB input might be okay but we should make decisions based on users and roles.
* There is an extension that removes the need to lock tables to repack / vaccum. Figure out how it works!
* * https://github.com/reorg/pg_repack
* Investigate if the zheap table format would be better to implement.
** Until I get past a WAL implementation and planner costs I don't think its worth it.
** Since I extended the size of transaction IDs, I probably have a larger issue on my hands than normal postgres.
*** Reading into the zheap approach I'm thinking that I might have some space saving options availible for me. In particular if a tuple is frozen so its always availible I could remove the xmin/xmax and pack more into the page. Need more thinking however my approach of questioning the storage efficency of each part of data seems to be worth it.

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

Rust's inability to treat enum variants as a type is a HUGE pain. I cheated and separated serialization from deserialization.

## Legal Stuff (Note I'm not a lawyer!)

I am explicitly striving for SQL+Driver compatibility with [PostgreSQL](https://www.postgresql.org) so things such as system tables and code that handles them will be named the same. I don't think this violates their [trademark policy](https://www.postgresql.org/about/policies/trademarks/) but if I am please just reach out to me! I have also gone with a pretty restrictive license but I'm not tied to it if that is causing an issue for others who are using the code.