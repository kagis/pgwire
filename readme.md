# pgwire

Under development

[![npm](https://img.shields.io/npm/v/pgwire.svg)](https://www.npmjs.com/package/pgwire) [![travis](https://travis-ci.com/kagis/pgwire.svg?branch=master)](https://travis-ci.com/kagis/pgwire)

PostgreSQL client library for Node.js

## Features

- Memory efficient data streaming
- [Logical replication using pgoutput protocol](test/test.js#L358)
- [Multi-statement queries](test/test.js#L38)
- Efficient bytea transfering
- Copy from stdin and to stdout
- Single round-trip queries for trusted connections
- [Pure js without dependencies](package.json#L36)
<!-- - True asynchronous -->
<!-- - Interchangeable connection strategies -->
<!-- - Session safety guards -->

## [Examples](test/test.js)

- Simple query protocol
- Extended query protocol
- Multi-statement extended query
- Execute many
- Streaming
- Multiplexing
- Copy from stdin
- Logical replication

```js
// run with env POSTGRES=postgres://<user>@<host>:<port>/<database>
const pg = require('pgwire');
const { rows } = await pg.query(`SELECT 'hello', 'world'`);
console.log(rows);
// [['hello', 'world']]
```

## API Reference

- .connectRetry() -> [IClient](#IClient)
- .connect() -> [IClient](#IClient)
- .pool() -> [IClient](#IClient)

pgwire itself is `pool(process.env.POSTGRES)` instance

### Connection URI options

`postgres://<user>@<host>:<port>/<database>?poolMaxConnections=0&idleTimeout=0`

- poolMaxConnections
- idleTimeout

### IClient

- .query()
- .queryStream()
- .queryExtended()
  - .parse()
  - .bind()
  - .execute()
  - .describeStatement()
  - .describePortal()
  - .closeStatement()
  - .closePortal()
  - .fetch()
  - .stream()
- .session()
- .logicalReplication()
  - .pgoutput()
  - .ack()
  - .ackImmediate()
  - .end()
