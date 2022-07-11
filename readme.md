# pgwire is

PostgreSQL client library for Deno and Node.js that exposes all features of wire protocol.

- Memory efficient data streaming
- Logical replication, including pgoutput protocol
- Copy from stdin and to stdout
- Query cancellation
- Implicit-transaction multi-statement queries
- Listen/Notify
- Query pipelining, single round trip
- Efficient bytea transferring
- Pure js without dependencies

# Connecting to PostgreSQL server

```js
import { pgconnect } from 'https://raw.githubusercontent.com/kagis/pgwire/main/mod.js';
const pg = await pgconnect('postgres://USER:PASSWORD@HOST:PORT/DATABASE');
```

https://www.postgresql.org/docs/11/libpq-connect.html#id-1.7.3.8.3.6

Good practice is to get connection URI from environment variable:

```js
// app.js
import { pgconnect } from 'https://raw.githubusercontent.com/kagis/pgwire/main/mod.js';
const pg = await pgconnect(Deno.env.get('POSTGRES'));
```

Set `POSTGRES` environment variable when run process:

```sh
$ POSTGRES='postgres://USER:PASSWORD@HOST:PORT/DATABASE' deno run --allow-env --allow-net app.js
```

`pgconnect()` function also accepts parameters as object:

```js
const pg = await pgconnect({
  hostname: '127.0.0.1',
  port: 5432,
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
});
```

Its possible to pass multiple connection URIs or objects to `pgconnect()` function. In this case actual connection parameters will be computed by merging all parameters in first-win priority. Following technique can be used to force specific parameters values or provide default-fallback values:

```js
const pg = await pgconnect(Deno.env.get('POSTGRES'), {
  // use default application_name if not set in env
  application_name: 'my-awesome-app',
});
```

Don't forget to `.end()` connection when you don't need it anymore:

```js
const pg = await pgconnect(Deno.env.get('POSTGRES'));
try {
  // use `pg`
} finally {
  await pg.end();
}
```

# Using pgwire in web server

```js
// app.js
import { serve } from 'https://deno.land/std@0.147.0/http/server.ts';
import { pgpool } from 'https://raw.githubusercontent.com/kagis/pgwire/main/mod.js';

const pg = pgpool(Deno.env.get('POSTGRES'));
async function handleRequest(_req) {
  const [greeting] = await pg.query(`SELECT 'hello world, ' || now()`);
  return new Response(greeting);
}

const aborter = new AbortController();
Deno.addSignalListener('SIGINT', _ => aborter.abort());
await serve(handleRequest, { port: 8080, signal: aborter.signal });
await pg.end();
```

```sh
$ POSTGRES='postgres://USER:PASSWORD@HOST:PORT/DATABASE?_poolSize=4&_poolIdleTimeout=5min' deno run --allow-env --allow-net app.js
```

When `_poolSize` is not set then a new connection is created for each query. This option makes possible to switch to external connection pool like pgBouncer.

# Querying

```js
const { rows } = await pg.query(`
  SELECT i, 'Mississippi'
  FROM generate_series(1, 3) i
`);
assertEquals(rows, [
  [1, 'Mississippi'],
  [2, 'Mississippi'],
  [3, 'Mississippi'],
]);
```

Function call and other single-value results can be accessed by array destructuring.

```js
const [scalar] = await pg.query(`SELECT current_user`);
assertEquals(scalar, 'postgres');
```

# Parametrized query

```js
const { rows } = await pg.query({
  statement: `
    SELECT i, $1
    FROM generate_series(1, $2) i
  `,
  params: [
    { type: 'text', value: 'Mississippi' }, // $1
    { type: 'int4', value: 3 }, // $2
  ],
});
assertEquals(rows, [
  [1, 'Mississippi'],
  [2, 'Mississippi'],
  [3, 'Mississippi'],
]);
```

TODO Why no interpolation API

# Multi-statement queries

Postgres allows to execute multiple statements within a single query.

```js
const {
  results: [
    , // skip CREATE TABLE category
    , // skip CREATE TABLE product
    { rows: categories },
    { rows: products },
  ],
} = await pg.query(`
  -- lets generate sample data
  CREATE TEMP TABLE category(id) AS VALUES
  ('fruits'),
  ('vegetables');

  CREATE TEMP TABLE product(id, category_id) AS VALUES
  ('apple', 'fruits'),
  ('banana', 'fruits'),
  ('carrot', 'vegetables');

  -- then select all generated data
  SELECT id FROM category ORDER BY id;
  SELECT id, category_id FROM product ORDER BY id;
`);

assertEquals(categories, [
  ['fruits'],
  ['vegetables'],
]);
assertEquals(products, [
  ['apple', 'fruits'],
  ['banana', 'fruits'],
  ['carrot', 'vegetables'],
]);
```

Postgres wraps multi-statement query into transaction implicitly. Implicit transaction does rollback automatically when error occures or does commit when all statements successfully executed. Multi-statement queries and implicit transactions are described here https://www.postgresql.org/docs/14/protocol-flow.html#PROTOCOL-FLOW-MULTI-STATEMENT

Top level `rows` accessor will contain rows returned by last SELECTish statement.
Iterator accessor will iterate over first row returned by last SELECTish statement.

```js
const retval = await client.query(`
  SELECT id FROM category;

  -- Lets select products before update.
  -- This is last SELECTish statement
  SELECT id, category_id FROM product ORDER BY id FOR UPDATE;

  -- UPDATE is not SELECTish (unless RETURING used)
  UPDATE product SET category_id = 'food';
`);

const { rows } = retval;
assertEquals(rows, [
  ['apple', 'fruits'],
  ['banana', 'fruits'],
  ['carrot', 'vegetables'],
]);

const [topProductId, topProductCategory] = retval;
assertEquals(topProductId, 'apple');
assertEquals(topProductCategory, 'fruits');
```

# Large datasets streaming

There are two ways of fetching query result.
First way is to call `await pg.query()`. In this case all rows will be loaded in memory.

Other way to consume result is to call `pg.stream()` and iterate over data chunks.

```js
const iterable = pg.stream(`SELECT i FROM generate_series(1, 2000) i`);
let sum = 0;
for await (const chunk of iterable)
for (const [i] of chunk.rows) {
  sum += i;
}
// sum of natural numbers from 1 to 2000
assertEquals(sum, 2001000);
```

`pg.stream()` accepts the same parameters as `pg.query()`, supports parametrized and multistatement queries.

TODO describe chunk shape

# Copy from stdin

If statement is `COPY ... FROM STDIN` then `stdin` parameter must be set.

```js
async function * generateData() {
  const utf8enc = new TextEncoder();
  for (let i = 0; i < 2; i++) {
    yield utf8enc.encode(i + '\t' + 'Mississipi' + '\n');
  }
}

await pg.query({
  statement: `COPY foo FROM STDIN`,
  stdin: generateData('/tmp/file'),
});
```

# Copy to stdout

```js
const upstream = await pg.stream({
  statement: `COPY foo TO STDOUT`,
});

const utf8dec = new TextDecoder();
let result = '';
for await (const chunk of upstream) {
  result += utf8dec.decode(chunk);
}

assertEquals(result,
  '1\tMississippi\n' +
  '2\tMississippi\n' +
  '3\tMississippi\n'
));
```

# Listen and Notify

https://www.postgresql.org/docs/11/sql-notify.html

```js
pg.onnotification = ({ pid, channel, payload }) => {
  try {
    console.log(pid, channel, payload);
  } catch (err) {
    // handle error or let process exit
  }
});
await pg.query(`LISTEN some_channel`);
```

TODO back preassure doc

# Simple and Extended query protocols

Postgres has two query protocols - simple and extended. Simple protocol allows to send multi-statement query as single script where statements are delimited by semicolon. **pgwire** utilizes simple protocol when `.query()` is called with a string in first argument:

```js
await pg.query(`
  CREATE TABLE foo (a int, b text);
  INSERT INTO foo VALUES (1, 'hello');
  COPY foo FROM STDIN;
  SELECT * FROM foo;
`, {
  // optional stdin for COPY FROM STDIN statements
  stdin: fs.createReadableStream('/tmp/file1.tsv'),
  // stdin also accepts array of streams for multiple
  // COPY FROM STDIN statements
  stdins: [
    fs.createReadableStream(...),
    fs.createReadableStream(...),
    ...
  ],
});
```

Extended query protocol allows to pass parameters for each statement so it splits statements into separate chunks. Its possible to use extended query protocol by passing one or more statement objects to `.query()` function:

```js
await pg.query({
  statement: `CREATE TABLE foo (a int, b text)`,
}, {
  statement: `INSERT INTO foo VALUES ($1, $2)`,
  params: [
    { type: 'int4', value: 1 },       // $1
    { type: 'text', value: 'hello' }, // $2
  ],
}, {
  statement: 'COPY foo FROM STDIN',
  stdin: fs.createReadableStream('/tmp/file1.tsv'),
}, {
  statement: 'SELECT * FROM foo',
});
```

# Logical replication

Logical replication is native PostgreSQL mechanism which allows your app to subscribe on data modification events such as insert, update, delete and truncate. This mechanism can be useful in different ways - replicas synchronization, cache invalidation or history tracking. https://www.postgresql.org/docs/11/logical-replication.html

Lets prepare database for logical replication. At first we need to configure PostgreSQL server to write enough information to WAL:

```sql
ALTER SYSTEM SET wal_level = logical;
-- then restart postgres server
```

We need to create replication slot for our app. Replication slot is PostgreSQL entity which behaves like a message queue of replication events.

```sql
SELECT pg_create_logical_replication_slot(
  'my-app-slot',
  'test_decoding' -- logical decoder plugin
);
```

Generate some modification events:

```sql
CREATE TABLE foo(a INT NOT NULL PRIMARY KEY, b TEXT);
INSERT INTO foo VALUES (1, 'hello'), (2, 'world');
UPDATE foo SET b = 'all' WHERE a = 1;
DELETE FROM foo WHERE a = 2;
TRUNCATE foo;
```

Now we are ready to consume replication messages:

```js
import { pgconnect } from 'https://raw.githubusercontent.com/kagis/pgwire/main/mod.js';

const pg = await pgconnect({ replication: 'database' }, Deno.env.get('POSTGRES'));
try {
  const replicationStream = await pg.logicalReplication({ slot: 'my-app-slot' });
  const utf8dec = new TextDecoder();
  for await (const { lastLsn, messages } of replicationStream) {
    for (const { lsn, data } of messages) {
      // consume message somehow
      console.log(lsn, utf8dec.decode(data));
    }
    replicationStream.ack(lastLsn);
  }
} finally {
  await pg.end();
}
```

# "pgoutput" logical replication decoder

Modification events go through plugable logical decoder before events emitted to client. Purpose of logical decoder is to serialize in-memory events structures into consumable message stream. PostgreSQL has two built-in logical decoders:

- `test_decoding` which emits human readable messages for debugging and testing,

- and production usable `pgoutput` logical decoder which adapts modification events for replicas synchronization. **pgwire** implements `pgoutput` messages parser.

```sql
CREATE TABLE foo(a INT NOT NULL PRIMARY KEY, b TEXT);
CREATE PUBLICATION "my-app-pub" FOR TABLE foo;

SELECT pg_create_logical_replication_slot(
  'my-app-slot',
  'pgoutput' -- logical decoder plugin
)
```

```js
import { pgconnect } from 'https://raw.githubusercontent.com/kagis/pgwire/main/mod.js';

const pg = await pgconnect({ replication: 'database' }, Deno.env.get('POSTGRES'));
try {
  const replicationStream = await pg.logicalReplication({
    slot: 'my-app-slot',
    options: {
      'proto_version': 1,
      'publication_names': 'my-app-pub',
    },
  });
  for await (const { lastLsn, messages } of replicationStream.pgoutputDecode()) {
    for (const pgomsg of messages) {
      // consume pgomsg
    }
    replicationStream.ack(lastLsn);
  }
} finally {
  await pg.end();
}
```

# .query()

```js
const pgresult = await conn.query(` SELECT 'hello', 'world' `);

// PgResult is Iterable, which iterates over
// first row of last SELECTish statement.
const [hello, world] = pgresult;
assertEquals(hello, 'hello');
assertEquals(world, 'world');

assertEquals(pgresult.rows, [['hello', 'world']]);
assertEquals(pgresult.status, 'SELECT 1');

for (const column of pgresult.columns) {
  column.name;
  column.binary;
  column.typeOid;
  column.typeMod;
  column.typeSize;
  column.tableSchema;
  column.tableColumn;
}

for (const sub of pgresult.results) {
  sub.rows;
  sub.status;

  // see pgresult.columns above
  sub.columns;
}

for (const notice of out.notices) {
  notice.severity;
  notice.message;
}
```

# .stream()

# pgoutput messages

```js
for await (const chunk of replstream.pgoutputDecode()) {

  // (string) Lsn of last received message.
  // Use it for replstream.ack() to confirm receipt of whole chunk.
  chunk.lastLsn;
  // (bigint) Time of last received message. Microseconds since unix epoch.
  chunk.lastTime;

  for (const pgomsg of chunk.messages) {
    // (string | null) Log Serial Number of message.
    // Use it for replstream.ack() to confirm receipt of message.
    // TODO describe nullability.
    pgomsg.lsn;
    // (bigint) The server's system clock at the time of transmission,
    // as microseconds since Unix epoch.
    pgomsg.time;

    switch (pgomsg.tag) {

      // Transaction start boundary.
      case 'begin':
        // (string) Equals to `.commitLsn` of following `commit` message
        // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275
        pgomsg.commitLsn;
        // (bigint) Microseconds since unix epoch.
        pgomsg.commitTime;
        // (number) Transaction id.
        pgomsg.xid;

      case 'origin':
        // (string)
        pgomsg.originName;
        // (string)
        pgomsg.originLsn;

      // Emitted for user defined types which are used
      // in following `relation` message
      case 'type':
        // (string)
        pgomsg.typeOid;
        // (string)
        pgomsg.typeSchema;
        // (string)
        pgomsg.typeName;

      // Relation (table) description.
      // Emmited once per relation before
      // `insert`/`update`/`delete`/`truncate` messages
      // which references this relation.
      case 'relation':
        // (number) pg_class reference.
        pgomsg.relationOid;
        // (string) Relation (table) schema name
        pgomsg.schema;
        // (string) Relation (table) name
        pgomsg.name:
        // ('default' | 'nothing'| 'full' | 'index')
        // https://www.postgresql.org/docs/14/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY
        pgomsg.replicaIdentity;
        // (string[]) Array of key columns names (column.flags & 0b1)
        pgomsg.keyColumns;
        // (object[]) Relation columns descriptions
        for (const column of pgomsg.columns) {
          // (number) 0b1 if column is part of replica identity
          column.flags;
          // (string)
          column.name;
          // (number)
          column.typeOid;
          // (number)
          column.typeMod;
          // (string | null)
          column.typeName;
          // (string | null)
          column.typeSchema;
        }

      case 'insert':
        // (object) Associated relation.
        pgomsg.relation;
        // (object)
        pgomsg.key;
        // (object) Inserted row values.
        pgomsg.after;

      case 'update':
        // (object) Associated relation.
        pgomsg.relation;
        // (object)
        pgomsg.key;
        // (object | null) If pgomsg.relation.replicaIdentity == 'full'
        // then gets row values before update, otherwise gets null
        pgomsg.before;
        // (object) Row values after update.
        // If pgomsg.relation.replicaIdentity != 'full'
        // then unchanged TOASTed values will be undefined.
        // https://www.postgresql.org/docs/14/storage-toast.html
        pgomsg.after;

      case 'delete':
        // (object) Associated relation.
        pgomsg.relation;
        // (object)
        pgomsg.key;
        // (object | null) If pgomsg.relation.replicaIdentity == 'full'
        // then gets deleted row values, otherwise gets null
        pgomsg.before;

      case 'truncate':
        // (boolean)
        pgomsg.cascade;
        // (boolean)
        pgomsg.restartIdentity;
        // (object[]) Truncated relations descriptions.
        pgomsg.relations;

      // pg_logical_emit_message
      // https://www.postgresql.org/docs/14/functions-admin.html#id-1.5.8.33.8.5.2.2.22.1.1.1
      case 'message':
        // (string)
        pgomsg.prefix;
        // (Uint8Array)
        pgomsg.content;
        // (string) Equals to lsn which `pg_logical_emit_message` returns.
        pgomsg.messageLsn;
        // (boolean)
        pgomsg.transactional;

      // Transaction end boundary.
      case 'commit':
        // (string) Equals to `.commitLsn` of preceding `begin` message.
        pgomsg.commitLsn;
        // (bigint) Microseconds since unix epoch.
        pgomsg.commitTime;
    }
  }
}
```
