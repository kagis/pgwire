# pgwire

Under development

[![Build Status](https://travis-ci.com/kagis/pgwire.svg?branch=master)](https://travis-ci.com/kagis/pgwire)
[![](https://img.shields.io/npm/v/pgwire.svg)](https://www.npmjs.com/package/pgwire)

Low level PostgreSQL client lib for Node.js

- memory efficient data streaming
- logical replication
- pure js, zero dependencies

## Simple query protocol

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });
const cursor = conn.query(` SELECT 'hello', 'world' UNION ALL SELECT 'foo', 'bar' `);
const results = await cursor.fetch();
conn.terminate();
const [first_result] = results;
console.log(first_result.rows); // [['hello', 'world'], ['foo', 'bar']]
```

## Extended query protocol

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });
const cursor = (
  conn
  // prepare unnamed statement
  .parse(` SELECT 'hello', $1 `)
  // bind unnamed stamement to unnamed portal
  .bind({ params: ['world'] })
  // execute unnamed portal
  .execute()
  // end query batch
  .sync()
);
const results = await cursor.fetch();
const [first_result] = results;
console.log(first_result.rows); // [['hello', 'world']]
```

## Multi-statement extended query

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });
const cursor = (
  conn
  .parse(`SELECT 'hello'`).bind().execute()
  .parse(`SELECT 'world'`).bind().execute()
  .sync()
);
const results = await cursor.fetch();
const [first_result, second_results] = results;
console.log(first_result.rows); // [['hello']]
console.log(second_result.rows); // [['world']]
```

## Execute many

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });
const cursor = (
  conn
  .parse(`SELECT 'hello', $1`)
  .bind({ params: ['world'] })
  .execute()
  .bind({ params: ['other'] })
  .execute()
  .sync()
);
const results = await cursor.fetch();
const [first_result, second_results] = results;
console.log(first_result.rows); // [['hello', 'world']]
console.log(second_result.rows); // [['hello', 'other']]
```

## Streaming

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });
const cursor = (
  conn
  .parse(`SELECT generate_series(0, 999999)`)
  .bind()
  .execute()
  .sync()
);
for await (const msg of cursor) {
  if (msg.tag == 'DataRow') {
    console.log(msg.data);
  }
}
// [['0']]
// [['1']]
// [['2']]
// ...
```

## Multiplexing

```js
const { pgconnect } = require('pgwire');
const conn = await pgconnect({ url: 'postgres://USER@HOST:PORT/DATABASE' });

await (
  conn
  // start explicit transaction to preserve portals between Sync commands
  .parse('BEGIN').bind().execute()
  .parse({ name: 'stmt50', statement: `SELECT generate_series( 1, 50)` })
  .bind({ name: 'stmt50', portal: 'portal50' })
  .parse({ name: 'stmt99', statement: `SELECT generate_series(50, 99)` })
  .bind({ name: 'stmt99', portal: 'portal99' })
  .sync()
  .fetch()
);

let some_portal_was_suspended;
do {
  const [first_result, second_result] = await (
    conn
    .execute({ portal: 'portal50', limit: 10 })
    .execute({ portal: 'portal99', limit: 10 })
    .sync()
    .fetch()
  );
  console.log(first_result.rows);
  console.log(second_result.rows);
  some_portal_was_suspended = first_result.suspended || second_result.suspended;
} while (some_portal_was_suspended);
```
