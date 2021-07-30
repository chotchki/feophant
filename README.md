# FeOphant

A SQL database server written in Rust and inspired by PostreSQL.

Just a toy for the moment, but I'm actively working to fix that!

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

## What works user facing
You can currently start the server, connect to it and have it throw tons of errors. To support more there is a ton of infrastructure required to wire up next steps.

## Current TODO List - Subject to constant change!

**Path to 0.7.1**

Have another change to chew on, and its an even bigger refactoring. I should move types out of the constants package AND make it a trait.

Why? I have a couple reoccuring issues:
* Organization Problems
* * I don't like that its in the constants namespace, will probably fix this first.
* * Due to enums and types not being unified I have a zero byte mapper type that's mixed in the same file. I need to break that out.
* Design problems
* * I need to be able to determine what the serialized size of a tuple is. I cannot do that without actually doing the serialization.
* * Right now there is not a way to define custom composite types. This is important because there is not a way to support user defined types right now.
* * I have no way to support array types either.
* * Need a better way to track definition of column+type for a view into a table.

I'm thinking about the following design to fix this:

enum SqlType {
    Base(Option<BaseSqlTypes>),
    Composite(Vec<Option<BaseSqlTypes>>),
    Array(Vec<Vec<Option<BaseSqlTypes>>>)
}

enum SqlTypeMapper {
    Base,
    Composite,
    Array
}

enum BaseSqlTypes {
    Bool(bool),
    Integer(int4),
    Text(String),
}

enum BaseSqlTypesMapper {
    Bool,
    Integer,
    Text
}

This would enable me to support the full scale of postgres types not too inefficently. I'm not happy with how I'm defining the Composite vs array types, I'm debating if there is too much overhead OR honestly the array type is probable okay.

I need to chew on storing data, passing it around vs tagging it.

Did some drawing, I think I need to split storing data vs interpreting it.

//Used to define the type but NOT store it
enum SqlTypeDefinition {
    Base(BaseSqlTypesMapper),
    Composite(Vec<(String, Arc<SqlType>)>),
    Array(Vec<Arc<SqlType>>)
}

//Used to parse/store the data, NOT able to understand it without a matching SqlTypeDefinition
struct SqlTuple(Vec<Option<BaseSqlTypes>>)

enum BaseSqlTypes {
    Bool(bool),
    Integer(int4),
    Text(String),
}

enum BaseSqlTypesMapper {
    Bool,
    Integer,
    Text
}

Types have been rewritten, committing so I can break everything and go back still.

I am partially through the process and am realizing I need a layer above visable row management to handle constraints (starting with null). I'm going to do this at time of rowdata creation (minus foreign keys).

I am restructuring the in memory view of tables/columns to disconnect from on disk.

I am losing my mental model of how to do complex types AND arrays. Need to re-visit when I've slept.

New version:
<pre>
                                                                                                                                               null

                                                                                                      
                                                                                                                   
                Table                   Table                    Table                     Table                Uuid 
+------------+    +     +------------+    +     +-------------+    +     +--------------+    +     +----------+   +   +----------+
|            | SqlTuple |            | SqlTuple |             | SqlTuple |              | RowData |          | Page|         |
|  Trigger   | -------> |  Security  |-------> |  Constraint |-------> |  VisibleRow| -------> |  Row|----> |  I/O|
|            |          |            |          |            |          |             |          |          |      |          |
|  Manager   | <------- |  Manager   | <------- |  Manager    | <------- |  Manager     | <------- |  Manager | <---- |  Manager |
|            | SqlTuple |            | SqlTuple |             | SqlTuple |              | RowData |          | Uuid  |         |
+------------+    +     +------------+    +     +-------------+    +     +--------------+    +     +----------+   +   +----------+
                Type                    Type           ^         Type           ^          Type                 Page
                                                       |                        |

                                                     Null                   Transaction   
                                                     Unique                 Manager
                                                     Custom
</pre>

**Path to 0.8**

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

Implemented stronger page checking to make sure we don't store something that won't work on the file system.

Back to indexes for now. I need to make a decision on how to handle them hitting the file system.
    Postgres uses a series of OIDs to map onto disk.

    I've been using uuids, I think I'm going to continue that. That would also solve the postgres fork approach.

I'll have to switch IOManager to use uuid instead of Table as a key. Upside, I'm basically already doing that. (done)

I'm chewing on splitting the in-memory table/column definitions from the on-disk view. In particular I could remove the circular dependancy I have right now.

**Path to 0.9**

Implement support for running a fuzzer against the code base to ensure we are keeping the code at a high quality.

**Path to 0.10**

Implement where clauses, will likely need to have to start tracing columns from analyizing through to later stages.

**Path to 0.11**

Implement delete for tuples

**Path to 0.12**

pgbench setup can run successfully, in memory

**Path to 0.13**

Ensure data about table structures is thread safe in the face of excessive Arc usage.

See where I can pass read only data by reference instead of uisng Arc everywhere

**Path to 0.14**

Support a row with more than 4kb of text in it.

**Path to 0.15**

Implement sorting.

**Path to 0.16**

Implement column aliasing

**Path to 0.17**

Implement subselect.

**Path to 0.18**

Implement Updates.

**Path to 0.19**

Did some reading on how the buffer manager works and my implementation seems to be firmly in the right direction. Take that knowledge and implement persistence

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


### Rust Notes

How to setup modules sanely: https://dev.to/stevepryde/intro-to-rust-modules-3g8k

Reasonable application error type creation: https://github.com/dtolnay/anyhow

Library Errors: https://github.com/dtolnay/thiserror

Rust's inability to treat enum variants as a type is a HUGE pain. I cheated and separated serialization from deserialization.

## Legal Stuff (Note I'm not a lawyer!)

I am explicitly striving for SQL+Driver compatibility with [PostgreSQL](https://www.postgresql.org) so things such as system tables and code that handles them will be named the same. I don't think this violates their [trademark policy](https://www.postgresql.org/about/policies/trademarks/) but if I am please just reach out to me! I have also gone with a pretty restrictive license but I'm not tied to it if that is causing an issue for others who are using the code.