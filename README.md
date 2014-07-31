rust-cql
========

This project is based on [yjh0502/rust-cql](https://github.com/yjh0502/rust-cql)

Cassandra Query Language version 3 (cql3) binary protocol implementation with rust-lang. It should work for versions [v1](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol.spec;hb=refs/heads/cassandra-1.2), [v2](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec) and [v3](https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v3.spec) of the protocol. It compiles with rust 0.11.

Native protocol is disabled in Cassandra 1.2. [Please enable the native protocol before start](http://www.datastax.com/docs/1.2/cql_cli/cql_binary_protocol)

**Disclaimer**: this software is in alpha state, so expect bugs and rust anti-patterns (this is my first code in rust)
