import { assertEquals, assertMatch, assertRejects } from 'https://deno.land/std@0.106.0/testing/asserts.ts';
import { delay } from 'https://deno.land/std@0.106.0/async/delay.ts';
import { readAll, readerFromIterable, copy } from 'https://deno.land/std@0.106.0/io/mod.ts';
import { pgconnect, pgpool } from './mod.js';


// await delay(2000); // wait for postgres up

// Deno.test('connectRetry', async _ => {
//   try {
//     const conn = await pgconnect('postgres://postgres:qwerty@google.com:5433/postgres?.connectRetry=5000');
//     conn.end();
//   } catch (err) {
//     console.log(err);
//     console.log('busy', err instanceof Deno.errors.NotFound)
//     // for (const x in err) {
//     //   console.log(x, err[x]);
//     // }
//   }
// });


Deno.test('hello', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  try {
    const { scalar } = await conn.query(/*sql*/ `select 'hello'`);
    assertEquals(scalar, 'hello');
  } finally {
    await conn.end();
  }
});


Deno.test('extended protocol', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  try {
    const { scalar } = await conn.query({
      statement: /*sql*/ `select 'hello'`
    });
    assertEquals(scalar, 'hello');
  } finally {
    await conn.end();
  }
});

// Deno.test('sync connection', async _ => {
//   const { connection } = pgconnect('postgres://postgres@postgres:5432/postgres');
//   try {
//     await connection.query(/*sql*/ `select 'hello'`);
//   } finally {
//     await connection.end();
//   }
// });

Deno.test('connection error during query', async _ => {
  const { connection } = pgconnect('postgres://invalid@postgres:5432/postgres');
  try {
    await assertRejects(
      _ => connection.query(/*sql*/ `select`),
      Error,
      '[PGERR_28000]'
    );
  } finally {
    await connection.end();
  }
});

Deno.test('throw when query after close', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  await conn.end();
  await assertRejects(
    _ => conn.query(/*sql*/ `select`),
    Error,
    'postgres connection closed',
  );
});

Deno.test('connection end idempotent', async _ => {
  const conn = pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  await Promise.all([conn.end(), conn.end()]);
  await conn.end();
});

Deno.test('copy', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  try {
    const copyUpstream = conn.query({
      statement: /*sql*/ `copy (select 'hello' from generate_series(1, 2)) to stdout`,
    });
    const copyDump = await readAll(readerFromIterable(copyUpstream));
    const copyDumpS = new TextDecoder().decode(copyDump);
    assertEquals(copyDumpS, 'hello\nhello\n');
  } finally {
    await conn.end();
  }
});













// Deno.test('pgliteral', async () => {
//   assert.deepStrictEqual(pgwire.pgliteral(`foo`), `'foo'`);
//   assert.deepStrictEqual(pgwire.pgliteral(`'foo'`), `'''foo'''`);
//   assert.deepStrictEqual(pgwire.pgliteral(null), `NULL`);
// });

// Deno.test('pgident', async () => {
//   assert.deepStrictEqual(pgwire.pgident(`foo`), `"foo"`);
//   assert.deepStrictEqual(pgwire.pgident(`"foo"`), `"""foo"""`);
//   assert.deepStrictEqual(pgwire.pgident(`public`, `foo`), `"public"."foo"`);
// });

// Deno.test('wait for ready', async () => {
//   const conn = await pgwire.connectRetry(process.env.POSTGRES);
//   conn.end();
// });

// Deno.test('iss1', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES, {
//     replication: 'database',
//   });
//   try {
//     await conn.query('CREATE_REPLICATION_SLOT test_2b265aa2 LOGICAL test_decoding');

//     console.log('1---------------')
//     const replstream = await conn.logicalReplication({ slot: 'test_2b265aa2' });
//     replstream.ackImmediate('0/0');
//     setTimeout(_ => replstream.destroy(), 1000);
//     // replstream.on('end', _ => console.log('------------------ end'));
//     await finished(replstream);

//     console.log('2---------------')
//     const replstream2 = await conn.logicalReplication({ slot: 'test_2b265aa2' });
//     replstream2.ackImmediate('0/0');
//     console.log({ readable: replstream2.readable });
//     await finished(replstream2);

//     console.log('3---------------')
//     const replstream3 = await conn.logicalReplication({ slot: 'test_2b265aa2' });
//     replstream3.ackImmediate('0/0');
//     await finished(replstream3);
//   } finally {
//     conn.end();
//   }
// });


Deno.test('connection with unexisting user', async _ => {
  const { scalar: userExists } = await (
    pgpool('postgres://postgres:secret@postgres:5432/postgres')
    .query(/*sql*/ `select exists(select 1 from pg_user where usename = 'unknown')`)
  );
  assertEquals(userExists, false);
  await assertRejects(
    _ => pgconnect('postgres://unknown:invalid@postgres:5432/postgres'),
    Error,
    '[PGERR_28000]',
  );
});

Deno.test('new connection per request when poolMaxConnection is unset', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { scalar: pid } = await pg.query(/*sql*/ `select pg_backend_pid()::text`);
  assertMatch(pid, /^\d+$/);
  const { scalar: terminated } = await pg.query(/*sql*/ `select pg_terminate_backend(${pid})::text`);
  assertEquals(terminated, 'false');
});

Deno.test('simple proto', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query(/*sql*/ `select 'hello'`);
  assertEquals(result, {
    inTransaction: false,
    rows: [['hello']],
    empty: false,
    suspended: false,
    scalar: 'hello',
    command: 'SELECT 1',
    notices: [],
    results: [{
      rows: [['hello']],
      scalar: 'hello',
      command: 'SELECT 1',
      notices: [],
      empty: false,
      suspended: false,
    }],
  });
});

Deno.test('simple proto multi stmt', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query(/*sql*/ `
    values ('a'), ('b');
    values ('c', 'd');
  `);
  assertEquals(result, {
    inTransaction: false,
    notices: [],
    rows: [['c', 'd']],
    scalar: 'c',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
    results: [{
      rows: [['a'], ['b']],
      command: 'SELECT 2',
      notices: [],
      scalar: 'a',
      empty: false,
      suspended: false,
    }, {
      rows: [['c', 'd']],
      command: 'SELECT 1',
      notices: [],
      scalar: 'c',
      empty: false,
      suspended: false,
    }],
  });
});

Deno.test('parallel queries', async _ => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
  try {
    const [{ scalar: r1 }, { scalar: r2 }] = await Promise.all([
      conn.query(/*sql*/ `select 'a'`),
      conn.query(/*sql*/ `select 'b'`),
    ]);
    assertEquals(r1, 'a');
    assertEquals(r2, 'b');
  } finally {
    await conn.end();
  }
});

Deno.test('extended proto', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query({
    statement: /*sql*/ `select $1`,
    params: [{
      type: 'text',
      value: 'hello',
    }],
  });
  assertEquals(result, {
    inTransaction: false,
    rows: [['hello']],
    scalar: 'hello',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
    notices: [],
    results: [{
      rows: [['hello']],
      command: 'SELECT 1',
      scalar: 'hello',
      notices: [],
      empty: false,
      suspended: false,
    }],
  });
});

Deno.test('multi-statement extended query', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query({
    statement: /*sql*/ `select 'a'`,
  }, {
    statement: /*sql*/ `select 'b'`,
  });
  assertEquals(result, {
    inTransaction: false,
    rows: [['b']],
    scalar: 'b',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
    notices: [],
    results: [{
      rows: [['a']],
      command: 'SELECT 1',
      scalar: 'a',
      notices: [],
      empty: false,
      suspended: false,
    }, {
      rows: [['b']],
      command: 'SELECT 1',
      scalar: 'b',
      notices: [],
      empty: false,
      suspended: false,
    }],
  })
});

Deno.test('portal suspended', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query({
    statement: /*sql*/ `select 'hello' from generate_series(0, 10)`,
    limit: 2,
  });
  assertEquals(result, {
    inTransaction: false,
    rows: [['hello'], ['hello']],
    notices: [],
    scalar: 'hello',
    command: undefined,
    empty: false,
    suspended: true,
    results: [{
      rows: [['hello'], ['hello']],
      notices: [],
      command: undefined,
      scalar: 'hello',
      empty: false,
      suspended: true,
    }],
  });
});

Deno.test('empty query', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const result = await pg.query({ statement: '' });
  assertEquals(result, {
    inTransaction: false,
    rows: [],
    notices: [],
    scalar: undefined,
    command: undefined,
    empty: true,
    suspended: false,
    results: [{
      rows: [],
      notices: [],
      scalar: undefined,
      command: undefined,
      suspended: false,
      empty: true,
    }],
  });
});

Deno.test('extended copy from stdin', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query({
    statement: /*sql*/ `begin`,
  }, {
    statement: /*sql*/ `create temp table test(foo text, bar text)`,
  }, {
    statement: /*sql*/ `copy test from stdin`,
    stdin: ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
  }, {
    statement: /*sql*/ `table test`,
  });
  assertEquals(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
  function utf8encode(s) {
    return new TextEncoder().encode(s);
  }
});

Deno.test('simple copy in', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query(/*sql*/ `
    CREATE TEMP TABLE test(foo TEXT, bar TEXT);
    COPY test FROM STDIN;
    SELECT * FROM test;
  `, {
    stdin: ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
  });
  assertEquals(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
  function utf8encode(s) {
    return new TextEncoder().encode(s);
  }
});

Deno.test('simple copy in 2', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query(/*sql*/ `
    CREATE TEMP TABLE test(foo TEXT, bar TEXT);
    COPY test FROM STDIN;
    COPY test FROM STDIN;
    SELECT * FROM test;
  `, {
    stdins: [
      ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
      ['3\t', 'hello\n', '4\t', 'world\n'].map(utf8encode),
    ],
  });
  assertEquals(rows, [
    ['1', 'hello'],
    ['2', 'world'],
    ['3', 'hello'],
    ['4', 'world'],
  ]);
  function utf8encode(s) {
    return new TextEncoder().encode(s);
  }
});

Deno.test('extended copy in missing 1', async _ => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  await assertRejects(
    _ => pg.query(
      { statement: /*sql*/ `BEGIN` },
      { statement: /*sql*/ `CREATE TEMP TABLE test(foo INT, bar TEXT)` },
      { statement: /*sql*/ `COPY test FROM STDIN` },
    ),
    Error,
    '[PGERR_57014]',
  );
});

Deno.test('simple copy in missing', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  await assertRejects(
    _ => pg.query(/*sql*/ `
        CREATE TEMP TABLE test(foo INT, bar TEXT);
        COPY test FROM STDIN;
        SELECT 'hello';
        COPY test FROM STDIN;
        COPY test FROM STDIN;
    `, {
      stdins: [
        ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
        ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
        // here should be third one
      ],
    }),
    Error,
    '[PGERR_57014]',
  );
  function utf8encode(s) {
    return new TextEncoder().encode(s);
  }
});

Deno.test('row decode simple', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query(/*sql*/ `
    SELECT null,
      true, false,
      'hello'::text,
      'hello'::varchar(100),
      '\\xcafebabe'::bytea,
      42::int2, -42::int2,
      42::int4, -42::int4,
      42::int8, -42::int8,
      36.6::float4, -36.6::float4,
      36.6::float8, -36.6::float8,
      jsonb_build_object('hello', 'world', 'num', 1),
      json_build_object('hello', 'world', 'num', 1),
      '1a/2b'::pg_lsn,
      'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
      ARRAY[1, 2, 3]::int[],
      ARRAY[1, 2, 3]::text[],
      ARRAY['"quoted"', '{string}', '"{-,-}"'],
      ARRAY[[1, 2], [3, 4]],
      '[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[],
      ARRAY[1, NULL, 2]
  `);
  assertEquals(rows, [[
    null,
    true, false,
    'hello',
    'hello',
    new Uint8Array([0xca, 0xfe, 0xba, 0xbe]),
    42, -42,
    42, -42,
    42n, -42n,
    36.6, -36.6,
    36.6, -36.6,
    { hello: 'world', num: 1 },
    { hello: 'world', num: 1 },
    '0000001A/0000002B',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    [1, 2, 3],
    ['1', '2', '3'],
    ['"quoted"', '{string}', '"{-,-}"'],
    [[1, 2], [3, 4]],
    [[[1, 2, 3], [4, 5, 6]]],
    [1, null, 2],
  ]]);
});

Deno.test('row decode extended', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query({
    statement: /*sql*/ `
      SELECT null, true, false,
        'hello'::text,
        'hello'::varchar(100),
        '\\xcafebabe'::bytea,
        42::int2, -42::int2,
        42::int4, -42::int4,
        42::int8, -42::int8,
        36.599998474121094::float4, -36.599998474121094::float4,
        36.6::float8, -36.6::float8,
        jsonb_build_object('hello', 'world', 'num', 1),
        json_build_object('hello', 'world', 'num', 1),
        '1a/2b'::pg_lsn,
        'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
        ARRAY[1, 2, 3]::int[],
        ARRAY[1, 2, 3]::text[],
        ARRAY['"quoted"', '{string}', '"{-,-}"'],
        ARRAY[[1, 2], [3, 4]],
        '[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[],
        ARRAY[1, NULL, 2]
    `,
  });
  assertEquals(rows, [[
    null, true, false,
    'hello',
    'hello',
    new Uint8Array([0xca, 0xfe, 0xba, 0xbe]),
    42, -42,
    42, -42,
    42n, -42n,
    36.599998474121094, -36.599998474121094,
    36.6, -36.6,
    { hello: 'world', num: 1 },
    { hello: 'world', num: 1 },
    '0000001A/0000002B',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    [1, 2, 3],
    ['1', '2', '3'],
    ['"quoted"', '{string}', '"{-,-}"'],
    [[1, 2], [3, 4]],
    [[[1, 2, 3], [4, 5, 6]]],
    [1, null, 2],
  ]]);
});

Deno.test('param explicit type', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows: [row] } = await pg.query({
    statement: /*sql*/ `
      SELECT pg_typeof($1)::text, $1,
        pg_typeof($2)::text, $2,
        pg_typeof($3)::text, $3->>'key',
        pg_typeof($4)::text, $4::text,
        pg_typeof($5)::text, $5::text,
        pg_typeof($6)::text, $6::varchar(100)
    `,
    params: [{
      type: 'int4',
      value: 1,
    }, {
      type: 'bool',
      value: true,
    }, {
      type: 'jsonb',
      value: {
        key: 'hello',
      },
    }, {
      type: 'text[]',
      value: ['1', '2', '3', null],
    }, {
      type: 'bytea[]',
      // value: ['x', 'y', 'z'], // FIXME handle non Uint8Arrays
      value: [
        new Uint8Array([0xca]),
        new Uint8Array([0xfe]),
        new Uint8Array([0xba]),
        new Uint8Array([0xbe]),
      ],
    }, {
      type: 'varchar',
      value: 'hello',
    }],
  });
  assertEquals(row, [
    'integer', 1,
    'boolean', true,
    'jsonb', 'hello',
    'text[]', '{1,2,3,NULL}',
    'bytea[]', '{"\\\\xca","\\\\xfe","\\\\xba","\\\\xbe"}',
    'character varying', 'hello',
  ]);
});

// Deno.test('listen/notify', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES);
//   try {
//     const response = new Promise((resolve, reject) => {
//       conn.on('notification', resolve);
//       conn.on('error', reject);
//     });
//     await conn.query(/*sql*/ `LISTEN test`);
//     psql(/*sql*/ `NOTIFY test, 'hello'`);
//     const { channel, payload } = await response;
//     assert.deepStrictEqual(channel, 'test');
//     assert.deepStrictEqual(payload, 'hello');
//   } finally {
//     conn.end();
//   }
// });

// Deno.test('CREATE_REPLICATION_SLOT issue', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES, {
//     replication: 'database',
//   });
//   try {
//     await Promise.all([
//       conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding'),
//       conn.query(/*sql*/ `SELECT 1`),
//     ]);
//     await Promise.all([
//       conn.query('CREATE_REPLICATION_SLOT crs_iss1 LOGICAL test_decoding'),
//       assert.rejects(conn.query(/*sql*/ `SELECT 1/0`)),
//       conn.query(/*sql*/ `SELECT 1`),
//     ]);
//   } finally {
//     conn.end();
//   }
// });

// Deno.test('logical replication', async () => {
//   const client = await pgwire.connect(process.env.POSTGRES, {
//     replication: 'database',
//   });
//   try {
//     await client.query(/*sql*/ `
//       SELECT pg_create_logical_replication_slot(
//         'test_2b265aa1',
//         'test_decoding',
//         temporary:=true
//       )
//     `);
//     await client.query(/*sql*/ `CREATE TABLE foo_2b265aa1 AS SELECT 1 a`);
//     const { rows: expected } = await client.query(/*sql*/ `
//       SELECT lsn, data
//       FROM pg_logical_slot_peek_changes('test_2b265aa1', NULL, NULL)
//     `);
//     const replstream = await client.logicalReplication({
//       slot: 'test_2b265aa1',
//       highWaterMark: 1000,
//     });
//     const lines = [];
//     const timer = setTimeout(_ => replstream.destroy(), 500);
//     for await (const { lsn, data } of replstream) {
//       lines.push([lsn, data.toString()]);
//       timer.refresh();
//     }
//     assert.deepStrictEqual(lines.length, 3);
//     assert.deepStrictEqual(lines, expected);

//     // connection should be reusable after replication end
//     const { scalar: one } = await client.query(/*sql*/ `SELECT 1`);
//     assert.deepStrictEqual(one, 1);
//   } finally {
//     client.end();
//   }
// });

// Deno.test('logical replication - async iter break', async () => {
//   const client = await pgwire.connect(process.env.POSTGRES, {
//     replication: 'database',
//   });
//   try {
//     await client.query(/*sql*/ `
//       SELECT pg_create_logical_replication_slot(
//         'test_1b305eaf',
//         'test_decoding',
//         temporary:=true
//       )
//     `);
//     await client.query(/*sql*/ `
//       CREATE TABLE foo_1b305eaf AS SELECT 1 a
//     `);
//     const replstream = await client.logicalReplication({
//       slot: 'test_1b305eaf',
//     });
//     for await (const _ of replstream) {
//       break;
//     }
//     // connection should be reusable after replication end
//     const { scalar: one } = await client.query(/*sql*/ `SELECT 1`);
//     assert.deepStrictEqual(one, 1);
//   } finally {
//     client.end();
//   }
// });

// Deno.test('logical replication pgoutput', async () => {
//   psql(/*sql*/ `
//     CREATE TABLE foo1(a INT NOT NULL PRIMARY KEY, b TEXT);
//     CREATE PUBLICATION pub1 FOR TABLE foo1;
//     COMMIT;
//     SELECT pg_create_logical_replication_slot('test1', 'pgoutput');
//     INSERT INTO foo1 VALUES (1, 'hello'), (2, 'world');
//     UPDATE foo1 SET b = 'all' WHERE a = 1;
//     DELETE FROM foo1 WHERE a = 2;
//     TRUNCATE foo1;
//   `);
//   const expectedRelation = {
//     relationid: Number(psql(/*sql*/ `SELECT 'public.foo1'::regclass::oid`)),
//     schema: 'public',
//     name: 'foo1',
//     replicaIdentity: 'd',
//     attrs: [
//       { flags: 1, name: 'a', typeid: 23, typemod: -1 },
//       { flags: 0, name: 'b', typeid: 25, typemod: -1 },
//     ],
//   };
//   const peekedChanges = JSON.parse(psql(/*sql*/ `
//     SELECT json_agg(jsonb_build_object(
//       'lsn', lpad(split_part(lsn::text, '/', 1), 8, '0') ||
//         '/' || lpad(split_part(lsn::text, '/', 2), 8, '0'),
//       'xid', xid::text::int
//     ))
//     FROM pg_logical_slot_peek_binary_changes('test1', NULL, NULL,
//       'proto_version', '1', 'publication_names', 'pub1')
//   `));
//   const client = pgwire.pool(process.env.POSTGRES);
//   const replstream = await client.logicalReplication({
//     slot: 'test1',
//     startLsn: '0/0',
//     options: {
//       'proto_version': 1,
//       'publication_names': 'pub1',
//     },
//   });
//   const pgomsgs = [];
//   for await (const pgomsg of replstream.pgoutput()) {
//     delete pgomsg.endLsn;
//     delete pgomsg.time;
//     delete pgomsg.finalLsn;
//     delete pgomsg.commitTime;
//     delete pgomsg.commitLsn;
//     delete pgomsg._endLsn;
//     pgomsgs.push(pgomsg);
//     if (pgomsg.tag == 'commit') {
//       break;
//     }
//   }
//   assert.deepStrictEqual(pgomsgs, [{
//     tag: 'begin',
//     xid: peekedChanges[0].xid,
//     lsn: peekedChanges.shift().lsn,
//   }, {
//     tag: 'relation',
//     lsn: (peekedChanges.shift().lsn, '00000000/00000000'), // why?
//     ...expectedRelation,
//   }, {
//     tag: 'insert',
//     lsn: peekedChanges.shift().lsn,
//     after: { a: 1, b: 'hello' },
//     relation: expectedRelation,
//   }, {
//     tag: 'insert',
//     lsn: peekedChanges.shift().lsn,
//     after: { a: 2, b: 'world' },
//     relation: expectedRelation,
//   }, {
//     tag: 'update',
//     lsn: peekedChanges.shift().lsn,
//     relation: expectedRelation,
//     before: null,
//     after: { a: 1, b: 'all' },
//   }, {
//     tag: 'delete',
//     lsn: peekedChanges.shift().lsn,
//     relation: expectedRelation,
//     keyOnly: true,
//     before: { a: 2, b: null },
//   }, {
//     tag: 'relation',
//     lsn: (peekedChanges.shift().lsn, '00000000/00000000'), // why?
//     ...expectedRelation,
//   }, {
//     tag: 'truncate',
//     lsn: peekedChanges.shift().lsn,
//     cascade: false,
//     restartSeqs: false,
//     relations: [expectedRelation],
//   }, {
//     tag: 'commit',
//     lsn: peekedChanges.shift().lsn,
//     flags: 0,
//   }]);
// });

// Deno.test('logical replication ack', async () => {
//   psql(/*sql*/ `
//     BEGIN;
//     CREATE TABLE acktest(a INT NOT NULL PRIMARY KEY);
//     CREATE PUBLICATION acktest FOR TABLE acktest;
//     COMMIT;

//     SELECT pg_create_logical_replication_slot('acktest', 'test_decoding');

//     BEGIN;
//     INSERT INTO acktest VALUES (1), (2);
//     COMMIT;
//     BEGIN;
//     INSERT INTO acktest VALUES (3), (4);
//     COMMIT;
//   `);
//   const changesCount = JSON.parse(psql(/*sql*/ `
//     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
//   `));
//   assert.deepStrictEqual(changesCount, 8);
//   const firstCommitLsn = JSON.parse(psql(/*sql*/ `
//     SELECT to_json(lsn)
//     FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
//     WHERE data LIKE 'COMMIT%'
//     LIMIT 1
//   `));
//   const client = pgwire.pool(process.env.POSTGRES);
//   const replstream = await client.logicalReplication({
//     slot: 'acktest',
//     startLsn: '0/0',
//   });
//   replstream.ack(firstCommitLsn);
//   replstream.destroy();
//   await finished(replstream);
//   const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
//     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
//   `));
//   assert.deepStrictEqual(changesCountAfterAck, 4);
// });

// Deno.test('logical replication ignore ack after destroy', async () => {
//   psql(/*sql*/ `
//     BEGIN;
//     CREATE TABLE acktest_1(a INT NOT NULL PRIMARY KEY);
//     CREATE PUBLICATION acktest_1 FOR TABLE acktest_1;
//     COMMIT;

//     SELECT pg_create_logical_replication_slot('acktest_1', 'test_decoding');

//     BEGIN;
//     INSERT INTO acktest_1 VALUES (1), (2);
//     COMMIT;
//     BEGIN;
//     INSERT INTO acktest_1 VALUES (3), (4);
//     COMMIT;
//   `);
//   const changesCount = JSON.parse(psql(/*sql*/ `
//     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
//   `));
//   assert.deepStrictEqual(changesCount, 8);
//   const [firstCommitLsn, lastCommitLsn] = JSON.parse(psql(/*sql*/ `
//     SELECT json_agg(lsn)
//     FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
//     WHERE data LIKE 'COMMIT%'
//   `));
//   const client = pgwire.pool(process.env.POSTGRES);
//   const replstream = await client.logicalReplication({ slot: 'acktest_1' });
//   replstream.ack(firstCommitLsn);
//   replstream.destroy();
//   replstream.ack(lastCommitLsn);
//   await finished(replstream);
//   const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
//     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
//   `));
//   assert.deepStrictEqual(changesCountAfterAck, 4);
// });

// Deno.test('logical replication invalid startLsn', async () => {
//   const client = pgwire.pool(process.env.POSTGRES);
//   const response = client.logicalReplication({
//     slot: 'unicorn',
//     startLsn: 'invalid_lsn',
//   });
//   await assert.rejects(response, {
//     code: 'PGERR_INVALID_START_LSN',
//   });
// });

Deno.test('parse bind execute', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { scalar } = await pg.query({
    op: 'parse',
    statement: /*sql*/ `SELECT $1`,
    paramTypes: ['int4'],
  }, {
    op: 'bind',
    params: [{
      type: 'int4',
      value: 1,
    }],
  }, {
    op: 'execute',
  });
  assertEquals(scalar, 1);
});

// xDeno.test('connection session', async () => {
//   psql(/*sql*/ `CREATE SEQUENCE test_sess`);
//   const conn = await pgwire.connect(process.env.POSTGRES);
//   try {
//     const shouldBe1 = conn.session(async sess => {
//       await new Promise(resolve => setTimeout(resolve, 100));
//       return (
//         sess.query(/*sql*/ `SELECT nextval('test_sess')::int`)
//         .then(({ scalar }) => scalar)
//       );
//     });
//     const shouldBe2 = (
//       conn.query(/*sql*/ `SELECT nextval('test_sess')::int`)
//       .then(({ scalar }) => scalar)
//     );
//     assert.deepStrictEqual(
//       await Promise.all([shouldBe1, shouldBe2]),
//       [1, 2],
//     );
//   } finally {
//     conn.end();
//   }
// });

Deno.test('reject pending responses when connection close', async _ => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
  try {
    await Promise.all([
      assertRejects(_ => conn.query(/*sql*/ `SELECT pg_terminate_backend(pg_backend_pid())`)),
      assertRejects(_ => conn.query(/*sql*/ `SELECT`)),
    ]);
  } finally {
    await conn.end();
  }
});

Deno.test('notice', async _ => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
  try {
    const { results } = await conn.query(/*sql*/ `rollback`);
    assertEquals(results[0].notices[0].code, '25P01');
  } finally {
    await conn.end();
  }
});

// Deno.test('pgbouncer', async () => {
//   const pool = pgwire.pool(process.env.POSTGRES_PGBOUNCER);
//   const { scalar } = await pool.query(/*sql*/ `SELECT 1`);
//   assert.deepStrictEqual(scalar, 1);
// });

Deno.test('auth clear text', async _ => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      do $$
      begin
      create role u_clear login password 'qwerty';
      create temp table hba (lines text);
      insert into hba values ('host all postgres all trust');
      insert into hba values ('host all u_clear all password');
      execute format('copy hba to %L', current_setting('hba_file'));
      perform pg_reload_conf();
      end
      $$
    `);
  } finally {
    await conn0.end();
  }

  const conn = await pgconnect('postgres://u_clear:qwerty@postgres:5432/postgres');
  await conn.end();
});

Deno.test('auth md5', async _ => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      do $$
      begin
      create role u_md5 login password 'qwerty';
      create temp table hba as select unnest(array[
        'host all postgres all trust',
        'host all u_md5 all md5'
      ]);
      execute format('copy hba to %L', current_setting('hba_file'));
      perform pg_reload_conf();
      end
      $$
    `);
  } finally {
    await conn0.end();
  }
  const conn = await pgconnect('postgres://u_md5:qwerty@postgres:5432/postgres');
  await conn.end();
});

Deno.test('auth sha256', async () => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      do $$
      begin
      set password_encryption = 'scram-sha-256';
      create role u_sha256 login password 'qwerty';
      create temp table hba as select unnest(array[
        'host all postgres all trust',
        'host all u_sha256 all scram-sha-256'
      ]);
      execute format('copy hba to %L', current_setting('hba_file'));
      perform pg_reload_conf();
      end
      $$
    `);
  } finally {
    await conn0.end();
  }
  const conn = await pgconnect('postgres://u_sha256:qwerty@postgres:5432/postgres');
  await conn.end();
});

Deno.test('write after end', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  await conn.end();
  await assertRejects(_ => conn.query(/*sql*/ `select`));
});

// Deno.test('write after end 2', async () => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   conn.end();
//   await new Promise(resolve => setImmediate(resolve, 0));
//   await assert.rejects(conn.query(/*sql*/ `SELECT`));
// });

Deno.test('pool should reuse connection', async _ => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?poolSize=1');
  try {
    const { scalar: pid1 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    const { scalar: pid2 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    assertEquals(pid1, pid2);
  } finally {
    await pool.end();
  }
});

Deno.test('reset conn', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn.query(/*sql*/ `set idle_in_transaction_session_timeout = -1 `);
    await conn.query('begin; select 1').catch(Object);
    // await conn.query('START TRANSACTION READ ONLY; rollback;');
    // await delay(10000);
    await conn.query(' rollback;');
    // await conn.query(' create temp table hello as select 1;  select * from hello;')
    // await conn.query('discard all;')
  } finally {
    await conn.end();
  }
})

Deno.test('pool should auto rollback', async _ => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?poolMaxConnections=1');
  try {
    await pool.query(/*sql*/ `
      BEGIN;
      CREATE TABLE test(a TEXT);
    `);
    // if previous query was not rollbacked then next query will fail
    await pool.query(/*sql*/ `
      CREATE TABLE test(a TEXT);
      ROLLBACK;
    `);
  } finally {
    await pool.clear();
  }
});

Deno.test('pool - unexisting database', async () => {
  const pool = pgwire.pool({
    database: 'unicorn',
  }, process.env.POSTGRES);
  try {
    await assert.rejects(pool.query(/*sql*/ `SELECT`), {
      code: 'PGERR_3D000',
    });
  } finally {
    pool.clear();
  }
});

Deno.test('idle timeout', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES, {
    idleTimeout: 200,
  });
  try {
    await new Promise(resolve => conn.on('close', resolve));
  } finally {
    conn.end(); // close manually if fail
  }
});

Deno.test('idle timeout 2', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES, {
    idleTimeout: 200,
  });
  await Promise.all([
    conn.query(/*sql*/ `SELECT`),
    conn.query(/*sql*/ `SELECT`),
    conn.query(/*sql*/ `SELECT`),
  ]);
  try {
    await Promise.race([
      new Promise(resolve => conn.on('close', resolve)),
      new Promise((_, reject) => setTimeout(reject, 400, Error(
        'Connection was not closed after idleTimeout',
      ))),
    ]);
  } finally {
    conn.end(); // close manually if fail
  }
});

Deno.test('idle timeout 3', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES, {
    idleTimeout: 200,
  });
  await conn.query(/*sql*/ `SELECT`);
  await new Promise(resolve => setTimeout(resolve, 50));
  await conn.query(/*sql*/ `SELECT`);
  await new Promise(resolve => setTimeout(resolve, 50));
  await conn.query(/*sql*/ `SELECT`);
  try {
    await Promise.race([
      new Promise(resolve => conn.on('close', resolve)),
      new Promise((_, reject) => setTimeout(reject, 400, Error(
        'Connection was not closed after idleTimeout',
      ))),
    ]);
  } finally {
    conn.end();
  }
});

Deno.test('pool - idle timeout', async () => {
  const pool = pgwire.pool(process.env.POSTGRES, {
    idleTimeout: 200,
    poolMaxConnections: 1,
  });
  try {
    const { scalar: pid } = await pool.query(/*sql*/ `SELECT pg_backend_pid()`);
    const alive = Number(psql(/*sql*/ `
      SELECT count(*) FROM pg_stat_activity WHERE pid = '${pid}'
    `));
    assert.deepStrictEqual(alive, 1);
    await new Promise(resolve => setTimeout(resolve, 400));
    const stillAlive = Number(psql(/*sql*/ `
      SELECT count(*) FROM pg_stat_activity WHERE pid = '${pid}'
    `));
    assert.deepStrictEqual(stillAlive, 0, 'idleTimeout is not working');
  } finally {
    pool.clear();
  }
});

Deno.test('pool async error', async () => {
  const pool = pgwire.pool(process.env.POSTGRES, {
    poolMaxConnections: 1,
  });
  try {
    const { scalar: pid1 } = await pool.query(/*sql*/ `SELECT pg_backend_pid()`);
    psql(/*sql*/ `SELECT pg_terminate_backend('${pid1}')`);
    await new Promise(resolve => setTimeout(resolve, 200));
    const { scalar: pid2 } = await pool.query(/*sql*/ `SELECT pg_backend_pid()`);
    assert.notEqual(pid1, pid2);
  } finally {
    pool.clear();
  }
});

Deno.test('connection uri options', async () => {
  const conn = await pgwire.connect('postgres://postgres@postgres:5432/postgres?application_name=test');
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT current_setting('application_name')`);
    assert.equal(scalar, 'test');
  } finally {
    conn.end();
  }
});

Deno.test('idleTimeout=0 should not close connection', async () => {
  const conn = await pgwire.connect('postgres://postgres@postgres:5432/postgres');
  try {
    await new Promise(resolve => setTimeout(resolve, 200));
    await conn.query(/*sql*/ `SELECT`);
  } finally {
    conn.end();
  }
});

Deno.test('unix socket', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES_UNIX);
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT 'hello'`);
    assert.deepStrictEqual(scalar, 'hello');
  } finally {
    conn.end();
  }
});

Deno.test('copy to file', async () => {
  const client = pgwire.pool(process.env.POSTGRES);
  const resp = client.query(/*sql*/ `COPY (VALUES (1, 2)) TO STDOUT`);
  const fw = fs.createWriteStream('/tmp/test');
  await pipeline(resp, fw);
  const content = fs.readFileSync('/tmp/test', { encoding: 'utf-8' });
  assert.deepStrictEqual(content, '1\t2\n');
});

Deno.test('copy to stdout', async () => {
  const client = pgwire.pool(process.env.POSTGRES);
  const resp = client.query(/*sql*/ `COPY (VALUES (1, 2)) TO STDOUT`);
  const chunks = await readAllChunks(resp);
  assert.deepStrictEqual(String(Buffer.concat(chunks)), '1\t2\n');
});

Deno.test('copy to stdout 2', async () => {
  const client = pgwire.pool(process.env.POSTGRES);
  const resp = client.query(/*sql*/ `
    COPY (VALUES (1, 2)) TO STDOUT;
    COPY (VALUES (3, 4)) TO STDOUT;
  `);
  const chunks = await readAllChunks(resp);
  assert.deepStrictEqual(String(Buffer.concat(chunks)), '1\t2\n3\t4\n');
});

Deno.test('stream', async () => {
  const client = pgwire.pool(process.env.POSTGRES);
  const resp = client.query(/*sql*/ `SELECT 'hello' col`);
  const chunks = await readAllChunks(resp);
  assert.deepStrictEqual(chunks.shift().boundary, {
    tag: 'RowDescription',
    fields: [{
      name: 'col',
      tableid: 0,
      column: 0,
      typeid: 25,
      typelen: -1,
      typemod: -1,
      binary: 0,
    }],
  });
  assert.deepStrictEqual(chunks.shift(), ['hello']);
  assert.deepStrictEqual(chunks.shift().boundary, {
    tag: 'CommandComplete',
    command: 'SELECT 1',
  });
  assert.deepStrictEqual(chunks.shift().boundary, {
    tag: 'ReadyForQuery',
    transactionStatus: 73,
  });
  assert.deepStrictEqual(chunks.shift(), undefined);
});

Deno.test('stream destroy', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES);
  try {
    const resp = conn.query(/*sql*/ `SELECT generate_series(0, 2000)`);
    resp.destroy();
    const { scalar } = await conn.query(/*sql*/ `SELECT 'hello'`);
    assert.deepStrictEqual(scalar, 'hello');
  } finally {
    conn.end();
  }
});



Deno.test('cancel', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres?debug=true');
  try {
    // const response = conn.query(/*sql*/ `
    //   select 1, pg_sleep(5);
    //   select 2, pg_sleep(5);
    //   select 3, pg_sleep(5);
    //   select 4, pg_sleep(5);
    //   select 5, pg_sleep(5);
    //   select 6, pg_sleep(5);
    // `);
    const response = conn.query(
      { statement: /*sql*/ `select 1, pg_sleep(3)`},
      { statement: /*sql*/ `select 2, pg_sleep(3)`},
      { statement: /*sql*/ `select 3, pg_sleep(3)`},
      { statement: /*sql*/ `select 4, pg_sleep(3)`},
      { statement: /*sql*/ `select 5, pg_sleep(3)`},
      { statement: /*sql*/ `select 6, pg_sleep(3)`},
    );
    query: for await (const m of response) {
      for (const [i] of m.rows) {
        if (i > 2) {
          break query;
        }
      }
    }
  } finally {
    await conn.end();
  }
});
