rust-cql
========

This project is based on [neich/rust-cql](https://github.com/neich/rust-cql). It uses [mio](https://github.com/carllerche/mio) and [eventual](https://github.com/carllerche/eventual) to redesign it and extend his functionalities.

Cassandra Query Language version 3 (cql3) binary protocol implementation with rust-lang. It should work for versions [v1](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol.spec;hb=refs/heads/cassandra-1.2), [v2](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec) and [v3](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v3.spec) of the protocol. It compiles with Rust 1.9.

It uses Cargo as the build system and it includes a few tests, one for every functionality.


Please, take into account that the examples included has only been tested on Cassandra 2.1 running on Ubuntu 14.04 x64. It has not been tested on Windows or OS/X, or other versions or Cassandra.

Native protocol is disabled in some versions of Cassandra 1.2. [Please enable the native protocol before start](http://www.datastax.com/dev/blog/binary-protocol).


What works:
- Queries:
  - Execute queries
  - Create prepared queries
  - Execute prepared queries
  - Execute batch queries
- Asynchronous API using Futures from [eventual](https://github.com/carllerche/eventual).
- Load Balancing: Latency Aware and Round Robin policies
- Node auto discovery: queries to the first contact point to get the peers. Registers to that first node to track Cassandra's events.
- Connection Pooling: although there's only 1 connection per node (host). 

What doesn't work:
- Authentication
- SSL
- Pagination
- Sessions
- ...

Disclaimer: this software is in alpha state, so expect bugs and rust anti-patterns (this is my first code in rust).
