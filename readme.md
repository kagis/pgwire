# pgwire

Under development

[![npm](https://img.shields.io/npm/v/pgwire.svg)](https://www.npmjs.com/package/pgwire) [![travis](https://travis-ci.com/kagis/pgwire.svg?branch=master)](https://travis-ci.com/kagis/pgwire)

PostgreSQL client library for Node.js

## Features

- Memory efficient data streaming
- Logical replication using pgoutput protocol
- Copy from stdin and to stdout
- Multi-statement queries
- True asynchronous
- Pure js without dependencies
<!-- - Bandwidth efficient binary transfering -->
<!-- - Single round-trip requests for trusted pgBouncer connections -->

## ![Examples](test/test.js)

- Simple query protocol
- Extended query protocol
- Multi-statement extended query
- Execute many
- Streaming
- Multiplexing
- Copy from stdin
- Logical replication

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://<user>@<host>:<port>/<database>' });
const { rows } = await conn.query(`SELECT 'hello', 'world'`);
console.log(rows);
// [['hello', 'world']]
```

## API Reference

- pgconnect()
  - .query()
  - .queryStream()
  - .extendedQuery()
    - .parse()
    - .bind()
    - .execute()
    - .describeStatement()
    - .describePortal()
    - .closeStatement()
    - .closePortal()
    - .fetch()
    - .stream()
  - .end()
  - .logicalReplication()
    - .pgoutput()
    - .ack()
    - .ackImmediate()
    - .end()
- pgliteral()
- pgident()
