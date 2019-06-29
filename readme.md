# PGWIRE

PostgreSQL client library for Node.js

[![npm](https://img.shields.io/npm/v/pgwire.svg)](https://www.npmjs.com/package/pgwire) [![travis](https://travis-ci.com/kagis/pgwire.svg?branch=master)](https://travis-ci.com/kagis/pgwire)

Under development

## Features

- Memory efficient data streaming
- Logical replication using pgoutput protocol
- Multi-statement queries
- Efficient bytea transfering
- Copy from stdin and to stdout
- SCRAM-SHA-256 authentication method support
- [Pure js without dependencies](package.json#L36)
<!-- - True asynchronous -->

# Connecting to PostgreSQL server

```js
import pgwire from 'pgwire';
const client = await pgwire.connect(
  'postgres://USER:PASSWORD@HOST:PORT/DATABASE?application_name=my-app'
);
```

https://www.postgresql.org/docs/11/libpq-connect.html#id-1.7.3.8.3.6

Good practice is to get connection URI from environment variable:

```js
// myapp.js
const client = await pgwire.connect(process.env.POSTGRES);
```

Set `POSTGRES` environment variable when run node process:

```bash
$ POSTGRES=postgres://USER:PASSWORD@HOST:PORT/DATABASE node myapp.js
```

`pgwire.connect()` function also accepts parameters as object:

```js
const client = await pgwire.connect({
  user: 'postgres',
  password: 'postgres',
  host: '127.0.0.1',
  port: 5432,
  database: 'postgres',
});
```

Its possible to pass multiple connection URIs or objects to `pgwire.connect()` function. In this case actual connection parameters will be computed by merging all parameters in last-win priority. Following technique can be used to set default connection parameters:

```js
const client = await pgwire.connect({
  application_name: 'my-awesome-app',
}, process.env.POSTGRES);
```

Don't forget to close connection when you don't need it anymore:

```js
const client = await pgwire.connect(process.env.POSTGRES);
try {
  // use client
} finally {
  client.end();
}
```

# Querying

```js
const { rows } = await client.query(`
  SELECT i, 'Mississippi'
  FROM generate_series(1, 3) i
`);

console.assert(JSON.stringify(rows) == JSON.stringify([
  [1, 'Mississippi'],
  [2, 'Mississippi'],
  [3, 'Mississippi'],
]));
```

Function call and other single-value query result can be accessed by `scalar` property. It equals to the first cell of the first row. If query returns no rows then `scalar` equals to `undefined`.

```js
const { scalar } = await client.query(`
  SELECT current_user
`);

console.assert(scalar == 'postgres');
```

# Parametrized query

```js
const { rows } = await client.query({
  statement: `SELECT $1, $2`,
  params: [{
    type: 'int4',
    value: 42,
  }, {
    type: 'text',
    value: 'hello',
  }],
});

console.assert(JSON.stringify(rows) == JSON.stringify([
  [42, 'hello'],
]));
```

# Multi-statement queries

Its possible to execute multiple statements within a single query.

```js
const {
  results: [
    { rows: categories },
    { rows: products },
  ],
} = await client.query({
  statement: `
    SELECT id, name
    FROM category
  `,
}, {
  statement: `
    SELECT id, category_id, name
    FROM product
  `,
});
```

Benefit of multi-statement queries is that statements execute in single round trip between your app and PostgreSQL server and is wrapped into transaction implicitly. Implicit transaction rolls back automatically when error occures and commits when all statements succesfully executed.

# Copy from stdin

```js
await client.query({
  statement: `COPY foo FROM STDIN`,
  stdin: fs.createReadableStream('/tmp/file'),
});
```

If you want to use simple query protocol for some reason then it is possible by following api:

```js
await client.query(`COPY foo FROM STDIN`, {
  stdin: fs.createReadableStream('/tmp/file'),
});
```

Since simple query can have multiple `COPY FROM STDIN` statements so its possible to specify an array of source streams:

```js
await client.query(`
  COPY foo FROM STDIN;
  COPY bar FROM STDIN;
`, {
  stdin: [
    fs.createReadableStream('/tmp/file1'),
    fs.createReadableStream('/tmp/file2'),
  ],
});
```

But if you need multi-statement COPY FROM query so extended query protocol is preferred way:

```js
await client.query({
  statement: `COPY foo FROM STDIN`,
  stdin: fs.createReadableStream('/tmp/file1'),
}, {
  statement: `COPY bar FROM STDIN`,
  stdin: fs.createReadableStream('/tmp/file2'),
});
```

# Reading from stream

`.query()` response can be consumed in two ways. First way is to `await` response like a promise (or call `response.then` explicitly). In this case all rows will be loaded in memory.

Other way to consume response is to use it as Readable stream.

```js
// omit `await` keyword to get underlying response stream
const response = client.query(`
  SELECT i FROM generate_series(1, 2000) i
`);
let sum = 0;
for await (const chunk of response) {
  if (chunk.boundary) {
    // This chunks are useful to determine
    // multi-statement results boundaries.
    // But we don't need it now so
    continue;
  }
  const [i] = chunk;
  sum += i;
}
// prints sum of natural numbers from 1 to 2000
console.log(sum);
```

# Listen and Notify

```js
client.on('notify', ({ channel, payload }) => {
  console.log(channel, payload);
});
await client.query(`LISTEN some_channel`);
```

# Extended query protocol

pgwire exposes postgres extended query protocol api

```js
.query({
  statement: 'SELECT $1, $2',
  params: [{
    type: 'int4',
    value: 42,
  }, {
    type: 25,
    value: 'hello',
  }],
  limit: 10,
  stdin: fs.createReadStream(...),
}, {
  // creates prepared statement
  op: 'parse',
  // sql statement body
  statement: 'SELECT $1, $2',
  // optional statement name
  // if not specified then unnamed statement will be created
  statementName: 'example-statement',
  // optional parameters types hints
  // if not specified then parameter types will be inferred
  paramTypes: [
    // can use builtin postgres type names
    'int4',
    // also can use any type oid directly
    25,
  ],
}, {
  // creates portal (cursor) from prepared statement
  op: 'bind',
  // optional statement name
  // if not specified then last unnamed prepared statement
  // will be used
  statementName: 'example-statement',
  // optional portal name
  // if not specified then unnamed portal will be created
  portal: 'example-portal',
  // optional parameters list
  params: [{
    // optional builtin parameter type name
    // if not specified then value should be
    // string or Buffer representing raw text or binary
    // encoded value
    type: 'int4',
    // $1 parameter value
    value: 1,
  }, {
    // also can use builtin type oid
    type: 25,
    // $2 parameter value
    value: 'hello',
  }],
}, {
  // executes portal
  op: 'execute',
  // optional portal name
  // if not specified then last unnamed portal will be executed
  portal: 'example-portal',
  // optional row limit value
  // if not specified then all rows will be returned
  // if limit is exceeded then result will have `suspeded: true` flag
  limit: 10,
  // optional source stream for `COPY target FROM STDIN` queries
  stdin: fs.createReadStream(...),
});
```

# Connection pool

```js
const client = pgwire.pool(
  'postgres://USER:PASSWORD@HOST:PORT/DATABASE?poolMaxConnections=4&idleTimeout=900000'
);
```

# Logical replication

Logical replication is native PostgreSQL mechanism which allows your app to subscribe on data modification events such as insert, update, delete and truncate. This mechanism can be useful in different ways - replicas synchronization, cache invalidation or history tracking. https://www.postgresql.org/docs/11/logical-replication.html

Lets prepare database for logical replication

```sql
CREATE TABLE foo(a INT NOT NULL PRIMARY KEY, b TEXT);
```

Replication slot is PostgreSQL entity which behaves like a message queue of replication events. Lets create replication slot for our app.

```sql
SELECT pg_create_logical_replication_slot(
  'my-app-slot',
  'test_decoding' -- logical decoder plugin
);
```

Generate some modification events:

```sql
INSERT INTO foo VALUES (1, 'hello'), (2, 'world');
UPDATE foo SET b = 'all' WHERE a = 1;
DELETE FROM foo WHERE a = 2;
TRUNCATE foo;
```

Now we are ready to consume replication events.

```js
import pgwire from 'pgwire';
const client = await pgwire.connect(process.env.POSTGRES, {
  replication: 'database',
});

const replicationStream = await client.logicalReplication({
  slot: 'my-app-slot',
  startLsn: '0/0',
});

for await (const { lsn, data } of replicationStream) {
  console.log(logicalEvent.toString());
  replicationStream.ack(lsn);
}
```

# "pgoutput" logical replication decoder

Modification events go through plugable logical decoder at first before events emitted to client. PostgreSQL has two built-in logical decoders - `test_decoding` which emits human readable events for debug, and `pgoutput` logical decoder which is used for replicas synchronization.

**pgwire** implements `pgoutput` events parser

```sql
CREATE PUBLICATION "my-app-pub" FOR TABLE foo
```

```sql
SELECT pg_create_logical_replication_slot(
  'my-app-slot',
  'pgoutput' -- logical decoder plugin
)
```

```js
import pgwire from 'pgwire';
const client = await pgwire.connect(process.env.POSTGRES, {
  replication: 'database',
});

const replicationEvents = await client.logicalReplication({
  slot: 'my-app-slot',
  startLsn: '0/0',
  options: {
    'proto_version': 1,
    'publication_names': 'my-app-pub',
  },
});

const pgoEvents = replicationEvents.pgoutput();

for await (const logicalEvent of pgoEvents) {
  switch (logicalEvent.tag) {
    case 'begin':
    case 'relation':
    case 'type':
    case 'origin':
    case 'insert':
    case 'update':
    case 'delete':
    case 'truncate':
    case 'commit':
  }
}
```
