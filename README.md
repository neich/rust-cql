rust-cql
========

This project is based on [yjh0502/rust-cql](https://github.com/yjh0502/rust-cql)

Cassandra Query Language version 3 (cql3) binary protocol implementation with rust-lang. It should work for versions [v1](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol.spec;hb=refs/heads/cassandra-1.2), [v2](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec) and [v3](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v3.spec) of the protocol. It compiles with rust 0.13-nighly, so it is incompatible with previous versions of rust.

It uses Cargo as the build system and it includes a VERY simple integration test.

This is a low level driver that does not implement fancy features like node auto discovery or load balancing.

Please, take into account that the (very small) example included has only been tested on Cassandra 1.2.18, 2.0.9, and 2.1.0 running on Ubuntu 14.04 x86. It has not been tested on Windows or OS/X, or other versions or Cassandra.

Native protocol is disabled in some versions of Cassandra 1.2. [Please enable the native protocol before start](http://www.datastax.com/dev/blog/binary-protocol).

What works:
- Execute queries
- Create prepared queries
- Execute prepared queries

What doesn't work:
- Decimal and Varint types
- Authentication
- SSL
- Pagination
- ...

**Disclaimer**: this software is in alpha state, so expect bugs and rust anti-patterns (this is my first code in rust). 
