export function setup({
  pgconnect,
  pgpool,
  test,
  assertEquals,
}) {

  test('simple proto', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const res = await conn.query(/*sql*/ `select 'hello' a`);
      assertEquals(Array.from(res), ['hello']);
      assertEquals(res.scalar, 'hello');
      assertEquals(res.rows, [['hello']]);
      assertEquals(res.command, 'SELECT 1');
      assertEquals(res.empty, false);
      assertEquals(res.suspended, false);
      assertEquals(res.notices, []);
      assertEquals(res.columns, [{
        binary: 0,
        name: 'a',
        tableColumn: 0,
        tableOid: 0,
        typeMod: -1,
        typeOid: 25,
        typeSize: 65535,
      }]);
    } finally {
      await conn.end();
    }
  });

// test('scalar accessor', async _ => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   try {
//     const { scalar } = await conn.query(/*sql*/ `
//       select 'a';
//       select 'hello', 'b';
//     `);
//     assertEquals(scalar, 'hello');
//   } finally {
//     await conn.end();
//   }
// });

test('simple proto multi statement', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const resultset = await conn.query(/*sql*/ `
      values ('a'), ('b');
      values ('c', 'd');
    `);
    assertEquals(Array.from(resultset), [{
      scalar: 'a',
      first: ['a'],
      rows: [['a'], ['b']],
      command: 'SELECT 2',
      // notices: [],
      empty: false,
      suspended: false,
    }, {
      scalar: 'c',
      first: ['c', 'd'],
      rows: [['c', 'd']],
      command: 'SELECT 1',
      // notices: [],
      empty: false,
      suspended: false,
    }]);
  } finally {
    await conn.end();
  }
});

// test('extended protocol', async _ => {
//   const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
//   try {
//     const { scalar } = await conn.query({
//       statement: /*sql*/ `select 'hello'`
//     });
//     assertEquals(scalar, 'hello');
//   } finally {
//     await conn.end();
//   }
// });

test('extended proto', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const resultset = await conn.query({
      statement: /*sql*/ `select $1`,
      params: [{
        type: 'text',
        value: 'hello',
      }],
    });
    assertEquals(Array.from(resultset), [{
      rows: [['hello']],
      command: 'SELECT 1',
      scalar: 'hello',
      notices: [],
      empty: false,
      suspended: false,
    }]);
  } finally {
    await conn.end();
  }
});

test('multi-statement extended query', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const result = await conn.query({
      statement: /*sql*/ `select 'a'`,
    }, {
      statement: /*sql*/ `select 'b'`,
    });
    assertEquals(result, [{
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
    }]);
  } finally {
    await conn.end();
  }
});

test('suspended', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const { suspended } = await conn.query({
      statement: /*sql*/ `select 'hello' from generate_series(0, 10)`,
      limit: 2,
    });
    assertEquals(suspended, true);
  } finally {
    await conn.end();
  }
});

test('empty', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const { empty: emptySimple } = await conn.query(/*sql*/ ``);
    assertEquals(emptySimple, true);
    const { empty: emptyExtended } = await conn.query({ statement: /*sql*/ `` });
    assertEquals(emptyExtended, true);
  } finally {
    await conn.end();
  }
});

// test('sync connection', async _ => {
//   const { connection } = pgconnect('postgres://postgres@postgres:5432/postgres');
//   try {
//     await connection.query(/*sql*/ `select 'hello'`);
//   } finally {
//     await connection.end();
//   }
// });

// test('connection error during query', async _ => {
//   const { connection } = pgconnect('postgres://invalid@postgres:5432/postgres');
//   try {
//     await assertRejects(
//       _ => connection.query(/*sql*/ `select`),
//       Error,
//       '[PGERR_28000]'
//     );
//   } finally {
//     await connection.end();
//   }
// });

// test('throw when query after close', async _ => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   await conn.end();
//   await assertRejects(
//     _ => conn.query(/*sql*/ `select`),
//     Error,
//     'postgres connection closed',
//   );
// });

test('connection end idempotent', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  await Promise.all([conn.end(), conn.end()]);
  await conn.end();
});













// test('pgliteral', async () => {
//   assert.deepStrictEqual(pgwire.pgliteral(`foo`), `'foo'`);
//   assert.deepStrictEqual(pgwire.pgliteral(`'foo'`), `'''foo'''`);
//   assert.deepStrictEqual(pgwire.pgliteral(null), `NULL`);
// });

// test('pgident', async () => {
//   assert.deepStrictEqual(pgwire.pgident(`foo`), `"foo"`);
//   assert.deepStrictEqual(pgwire.pgident(`"foo"`), `"""foo"""`);
//   assert.deepStrictEqual(pgwire.pgident(`public`, `foo`), `"public"."foo"`);
// });

// test('wait for ready', async () => {
//   const conn = await pgwire.connectRetry(process.env.POSTGRES);
//   conn.end();
// });

// test('iss1', async () => {
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


test('connect with unexisting user', async _ => {
  // const { scalar: userExists } = await (
  //   pgpool('postgres://postgres:secret@postgres:5432/postgres')
  //   .query(/*sql*/ `select exists(select from pg_user where usename = 'unknown')`)
  // );
  // assertEquals(userExists, false);
  const err = await pgconnect('postgres://unknown@postgres:5432/postgres').catch(Object);
  assertEquals(err.name, 'PgError.28000');
});

test('concurent queries', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
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

test('extended copy from stdin', async _ => {
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

test('simple copy in', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query(/*sql*/ `
    create temp table test(foo text, bar text);
    copy test from stdin;
    select * from test;
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

test('simple copy in 2', async () => {
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

test('extended copy in missing 1', async _ => {
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

test('simple copy in missing', async () => {
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

test('row decode simple', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query(String.raw /*sql*/ `
    SELECT null,
      true, false,
      'hello'::text,
      'hello'::varchar(100),
      '\xcafebabe'::bytea,
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
    Uint8Array.of(0xca, 0xfe, 0xba, 0xbe),
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

test('row decode extended', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { rows } = await pg.query({
    statement: String.raw /*sql*/ `
      SELECT null, true, false,
        'hello'::text,
        'hello'::varchar(100),
        '\xcafebabe'::bytea,
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
    `,
  });
  assertEquals(rows, [[
    null, true, false,
    'hello',
    'hello',
    Uint8Array.of(0xca, 0xfe, 0xba, 0xbe),
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

test('param explicit type', async () => {
  // const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const pg = await pgconnect('postgres://postgres:secret@postgres:5432/postgres?.debug=true');
  try {
    const [...row] = await pg.query({
      statement: /*sql*/ `
        select
        pg_typeof($1)::text, $1,
        pg_typeof($2)::text, $2,
        pg_typeof($3)::text, $3->>'key',
        pg_typeof($4)::text, encode($4, 'hex'),
        pg_typeof($5)::text, $5::text,
        pg_typeof($6)::text, $6::text,
        pg_typeof($7)::text, $7::varchar(100)
      `,
      params: [
        { type: 'int4', value: 1 },
        { type: 'bool', value: true },
        { type: 'jsonb', value: { key: 'hello' } },
        { type: 'bytea', value: Uint8Array.of(0xca, 0xfe, 0xba, 0xbe) },
        { type: 'text[]', value: ['1', '2', '3', null] },
        { type: 'bytea[]', value: [0xca, 0xfe, 0xba, 0xbe].map(x => Uint8Array.of(x)) },
        { type: 'varchar', value: 'hello' },
      ],
    });
    assertEquals(row, [
      'integer', 1,
      'boolean', true,
      'jsonb', 'hello',
      'bytea', 'cafebabe',
      'text[]', '{1,2,3,NULL}',
      'bytea[]', '{"\\\\xca","\\\\xfe","\\\\xba","\\\\xbe"}',
      'character varying', 'hello',
    ]);
  } finally {
    await pg.end();
  }
});

// test('listen/notify', async () => {
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

// test('CREATE_REPLICATION_SLOT issue', async () => {
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

test('logical_replication', async _ => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres?replication=database');
  try {
    await conn.query(/*sql*/ `
      select pg_create_logical_replication_slot('test_2b265aa1', 'test_decoding', temporary:=true)
    `);
    await conn.query(/*sql*/ `create table foo_2b265aa1 as select 1 a`); // generate changes
    const { scalar: currentLsn } = await conn.query(/*sql*/ `select pg_current_wal_lsn()`);
    const { rows: expected } = await conn.query(/*sql*/ `
      select lsn, data from pg_logical_slot_peek_changes('test_2b265aa1', null, null)
    `);
    assertEquals(expected.length, 3);

    const actual = [];
    const utf8dec = new TextDecoder();
    const replstream = conn.logicalReplication({ slot: 'test_2b265aa1' });

    outer: for await (const chunk of replstream)
    for (const { lsn, data } of chunk) {
      actual.push([lsn, utf8dec.decode(data)]);
      if (lsn >= currentLsn) {
        break outer;
      }
    }
    assertEquals(actual, expected);

    // connection should be reusable after replication end
    const { scalar: hello } = await conn.query(/*sql*/ `select 'hello'`);
    assertEquals(hello, 'hello');
  } finally {
    await conn.end();
  }
});

test('logical_replication_pgoutput', async t => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres?replication=database&.debug=true');
  try {
    await Promise.all([
      conn.query(/*sql*/ `create table pgo1rel(id int not null primary key, val text, note text)`),
      conn.query(/*sql*/ `alter table pgo1rel alter column note set storage external`),
      conn.query(/*sql*/ `create publication pgo1pub for table pgo1rel`),
      conn.query(/*sql*/ `select pg_replication_origin_create('pgo1origin')`),
      conn.query(`CREATE_REPLICATION_SLOT pgo1slot TEMPORARY LOGICAL pgoutput`),
      // conn.query(/*sql*/ `select pg_replication_origin_session_setup('pgo1origin')`),
      // generate changes
      conn.query(/*sql*/ `begin`),
      conn.query(/*sql*/ `insert into pgo1rel select 1, 'foo', repeat('_toasted_', 10000)`),
      // toasted column unchanged, after.note == undefined expected
      conn.query(/*sql*/ `update pgo1rel set val = 'bar'`),
      // key changed
      conn.query(/*sql*/ `update pgo1rel set id = 2`),
      conn.query(/*sql*/ `delete from pgo1rel`),
      conn.query(/*sql*/ `alter table pgo1rel replica identity full`),
      conn.query(/*sql*/ `insert into pgo1rel select 1, 'foo', repeat('_toasted_', 10000)`),
      // toasted column unchanged, but replica identity is full, so after.note == '_toasted_....' expected
      conn.query(/*sql*/ `update pgo1rel set val = 'bar'`),
      conn.query(/*sql*/ `delete from pgo1rel`),
      conn.query(/*sql*/ `truncate pgo1rel`),
      conn.query(/*sql*/ `select set_config('pgo1test.message_lsn', lsn::text, false) from pg_logical_emit_message(true, 'testmessage', 'hello world'::bytea) lsn`),
      conn.query(/*sql*/ `commit`),
    ]);
    const [stopLsn] = await conn.query(/*sql*/ `select pg_current_wal_lsn()`);
    const [messageLsn] = await conn.query(/*sql*/ `select current_setting('pgo1test.message_lsn')::pg_lsn`);

    const replstream = conn.logicalReplication({
      slot: 'pgo1slot',
      options: {
        proto_version: '1',
        publication_names: 'pgo1pub',
        // binary: 'true',
        messages: 'true',
      },
    });
    const actual = [];
    for await (const chunk of replstream.pgoutputDecode()) {
      // console.log(chunk);
      for (const pgomsg of chunk.messages) {
        actual.push(pgomsg);
      }
      // console.log({ lastLsn: chunk.lastLsn, stopLsn });
      if (chunk.lastLsn >= stopLsn) {
        break;
      }
    }
    // console.log(actual);

    const mbegin = actual.shift();
    assertEquals(typeof mbegin.lsn, 'string');
    assertEquals(typeof mbegin.time, 'bigint');
    assertEquals(mbegin.tag, 'begin');
    assertEquals(typeof mbegin.commitLsn, 'string');
    assertEquals(typeof mbegin.commitTime, 'bigint');
    assertEquals(typeof mbegin.xid, 'number');

    const mrel = actual.shift();
    assertEquals(mrel.lsn, null);
    assertEquals(typeof mrel.time, 'bigint');
    assertEquals(mrel.tag, 'relation');
    assertEquals(typeof mrel.relationOid, 'number');
    assertEquals(mrel.schema, 'public');
    assertEquals(mrel.name, 'pgo1rel');
    assertEquals(mrel.replicaIdentity, 'default');
    assertEquals(mrel.attrs, [
      { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
      { flags: 0, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
      { flags: 0, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' },
    ]);

    const minsert = actual.shift();
    assertEquals(typeof minsert.lsn, 'string');
    assertEquals(typeof minsert.time, 'bigint');
    assertEquals(minsert.tag, 'insert');
    assertEquals(minsert.relation, mrel);
    assertEquals(minsert.key, noproto({ id: 1 }));
    assertEquals(minsert.before, null);
    assertEquals(minsert.after, noproto({ id: 1, val: 'foo', note: '_toasted_'.repeat(10000) }));

    const mupdate = actual.shift();
    assertEquals(typeof mupdate.lsn, 'string');
    assertEquals(typeof mupdate.time, 'bigint');
    assertEquals(mupdate.tag, 'update');
    assertEquals(mupdate.relation, mrel);
    assertEquals(mupdate.key, noproto({ id: 1 }));
    assertEquals(mupdate.before, null);
    assertEquals(mupdate.after, noproto({ id: 1, val: 'bar', note: undefined }));

    const mupdate_ = actual.shift();
    assertEquals(typeof mupdate_.lsn, 'string');
    assertEquals(typeof mupdate_.time, 'bigint');
    assertEquals(mupdate_.tag, 'update');
    assertEquals(mupdate_.relation, mrel);
    assertEquals(mupdate_.key, noproto({ id: 1 }));
    assertEquals(mupdate_.before, null);
    assertEquals(mupdate_.after, noproto({ id: 2, val: 'bar', note: undefined }));

    const mdelete = actual.shift();
    assertEquals(typeof mdelete.lsn, 'string');
    assertEquals(typeof mdelete.time, 'bigint');
    assertEquals(mdelete.tag, 'delete');
    assertEquals(mdelete.relation, mrel);
    assertEquals(mdelete.key, noproto({ id: 2 }));
    assertEquals(mdelete.before, null);
    assertEquals(mdelete.after, null);

    const mrel2 = actual.shift();
    assertEquals(mrel2.lsn, null);
    assertEquals(typeof mrel2.time, 'bigint');
    assertEquals(mrel2.tag, 'relation');
    assertEquals(typeof mrel2.relationOid, 'number');
    assertEquals(mrel2.schema, 'public');
    assertEquals(mrel2.name, 'pgo1rel');
    assertEquals(mrel2.replicaIdentity, 'full');
    assertEquals(mrel2.attrs, [
      { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
      { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
      { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' },
    ]);

    const minsert2 = actual.shift();
    assertEquals(typeof minsert2.lsn, 'string');
    assertEquals(typeof minsert2.time, 'bigint');
    assertEquals(minsert2.tag, 'insert');
    assertEquals(minsert2.relation, mrel2);
    assertEquals(minsert2.key, noproto({ id: 1, val: 'foo', note: '_toasted_'.repeat(10000) }));
    assertEquals(minsert2.before, null);
    assertEquals(minsert2.after, noproto({ id: 1, val: 'foo', note: '_toasted_'.repeat(10000) }));

    const mupdate2 = actual.shift();
    assertEquals(typeof mupdate2.lsn, 'string');
    assertEquals(typeof mupdate2.time, 'bigint');
    assertEquals(mupdate2.tag, 'update');
    assertEquals(mupdate2.relation, mrel2);
    assertEquals(mupdate2.key, noproto({ id: 1, val: 'foo', note: '_toasted_'.repeat(10000) }));
    assertEquals(mupdate2.before, noproto({ id: 1, val: 'foo', note: '_toasted_'.repeat(10000) }));
    assertEquals(mupdate2.after, noproto({ id: 1, val: 'bar', note: '_toasted_'.repeat(10000) }));

    const mdelete2 = actual.shift();
    assertEquals(typeof mdelete2.lsn, 'string');
    assertEquals(typeof mdelete2.time, 'bigint');
    assertEquals(mdelete2.tag, 'delete');
    assertEquals(mdelete2.relation, mrel2);
    assertEquals(mdelete2.key, noproto({ id: 1, val: 'bar', note: '_toasted_'.repeat(10000) }));
    assertEquals(mdelete2.before, noproto({ id: 1, val: 'bar', note: '_toasted_'.repeat(10000) }));
    assertEquals(mdelete2.after, null);

    const mrel3 = actual.shift();
    assertEquals(mrel3.lsn, null);
    assertEquals(typeof mrel3.time, 'bigint');
    assertEquals(mrel3.tag, 'relation');
    assertEquals(typeof mrel3.relationOid, 'number');
    assertEquals(mrel3.schema, 'public');
    assertEquals(mrel3.name, 'pgo1rel');
    assertEquals(mrel3.replicaIdentity, 'full');
    assertEquals(mrel3.attrs, [
      { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
      { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
      { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' },
    ]);

    const mtruncate = actual.shift();
    assertEquals(typeof mtruncate.lsn, 'string');
    assertEquals(typeof mtruncate.time, 'bigint');
    assertEquals(mtruncate.tag, 'truncate');
    assertEquals(mtruncate.flags, 0);
    assertEquals(mtruncate.cascade, false);
    assertEquals(mtruncate.restartIdentity, false);
    assertEquals(mtruncate.relations, [mrel3]);

    const mmessage = actual.shift();
    assertEquals(typeof mmessage.lsn, 'string');
    assertEquals(typeof mmessage.time, 'bigint');
    assertEquals(mmessage.tag, 'message');
    assertEquals(mmessage.flags, 1);
    assertEquals(mmessage.transactional, true);
    assertEquals(mmessage.messageLsn, messageLsn);
    assertEquals(mmessage.prefix, 'testmessage');
    const helloworld = Uint8Array.from([104, 101, 108, 108, 111,  32, 119, 111, 114, 108, 100]);
    assertEquals(mmessage.content, helloworld);

    const mcommit = actual.shift();
    assertEquals(typeof mcommit.lsn, 'string');
    assertEquals(typeof mcommit.time, 'bigint');
    assertEquals(mcommit.tag, 'commit');
    assertEquals(mcommit.flags, 0);
    assertEquals(mcommit.commitTime, mbegin.commitTime);
    assertEquals(mcommit.commitLsn, mbegin.commitLsn);
    const mend = actual.shift();
    assertEquals(mend, undefined);

    // connection should be reusable after replication end
    const { scalar: hello } = await conn.query(/*sql*/ `select 'hello'`);
    assertEquals(hello, 'hello');
  } finally {
    await conn.end();
  }

  function noproto(obj) {
    return Object.assign(Object.create(null), obj);
  }
});



// test('logical replication - async iter break', async () => {
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

// test('logical replication pgoutput', async () => {
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

// test('logical replication ack', async () => {
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

// test('logical replication ignore ack after destroy', async () => {
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

// test('logical replication invalid startLsn', async () => {
//   const client = pgwire.pool(process.env.POSTGRES);
//   const response = client.logicalReplication({
//     slot: 'unicorn',
//     startLsn: 'invalid_lsn',
//   });
//   await assert.rejects(response, {
//     code: 'PGERR_INVALID_START_LSN',
//   });
// });

// test('_parse', async _ => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   try {
//     const resultset = await conn.query({
//       message: 'Parse',
//       statement: /*sql*/ `select $1`,
//       params: [{
//         type: 'text',
//         value: 'hello',
//       }],
//     });
//     console.log();
//     console.log(resultset);
//   } finally {
//     await conn.end();
//   }
// });

test('parse bind execute', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { scalar } = await pg.query({
    message: 'Parse',
    statement: /*sql*/ `SELECT $1`,
    paramTypes: ['int4'],
  }, {
    message: 'Bind',
    params: [{
      type: 'int4',
      value: 1,
    }],
  }, {
    message: 'Execute',
  });
  assertEquals(scalar, 1);
});

// test('connection session', async () => {
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

test('reject pending responses when connection close', async _ => {
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

test('notice', async _ => {
  const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
  try {
    const { results } = await conn.query(/*sql*/ `rollback`);
    assertEquals(results[0].notices[0].code, '25P01');
  } finally {
    await conn.end();
  }
});

// test('pgbouncer', async () => {
//   const pool = pgwire.pool(process.env.POSTGRES_PGBOUNCER);
//   const { scalar } = await pool.query(/*sql*/ `SELECT 1`);
//   assert.deepStrictEqual(scalar, 1);
// });

test('auth clear', async t => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      create role u_clear login password 'qwerty';
      create temp table hba as values ('host all postgres all trust'), ('host all u_clear all password');
      do $$ begin execute format('copy hba to %L', current_setting('hba_file')); end $$;
      select  pg_reload_conf();
    `);
  } finally {
    await conn0.end();
  }

  await assertRejects(async _ => {
    const conn = await pgconnect('postgres://u_clear@postgres:5432/postgres');
    conn.end();
  }, Error, 'password required (clear)');

  const conn1 = await pgconnect('postgres://u_clear:qwerty@postgres:5432/postgres');
  try {
    const { scalar } = await conn1.query(/*sql*/ `select current_user`);
    assertEquals(scalar, 'u_clear');
  } finally {
    await conn1.end();
  }
});

test('auth md5', async _ => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      set password_encryption = 'md5';
      create role u_md5 login password 'qwerty';
      create temp table hba as values ('host all postgres all trust'), ('host all u_md5 all md5');
      do $$ begin execute format('copy hba to %L', current_setting('hba_file')); end $$;
      select pg_reload_conf();
    `);
  } finally {
    await conn0.end();
  }

  await assertRejects(async _ => {
    const conn = await pgconnect('postgres://u_md5@postgres:5432/postgres');
    conn.end();
  }, Error, 'password required (md5)');

  const conn1 = await pgconnect('postgres://u_md5:qwerty@postgres:5432/postgres');
  try {
    const { scalar } = await conn1.query(/*sql*/ `select current_user`);
    assertEquals(scalar, 'u_md5');
  } finally {
    await conn1.end();
  }
});

test('auth scram-sha-256', async _ => {
  const conn0 = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    await conn0.query(/*sql*/ `
      set password_encryption = 'scram-sha-256';
      create role u_sha256 login password 'qwerty';
      create temp table hba as values ('host all postgres all trust'), ('host all u_sha256 all scram-sha-256');
      do $$ begin execute format('copy hba to %L', current_setting('hba_file')); end $$;
      select pg_reload_conf();
    `);
  } finally {
    await conn0.end();
  }

  await assertRejects(async _ => {
    const conn = await pgconnect('postgres://u_sha256@postgres:5432/postgres');
    conn.end();
  }, Error, 'password required (scram-sha-256)');

  const conn1 = await pgconnect('postgres://u_sha256:qwerty@postgres:5432/postgres');
  try {
    const { scalar } = await conn1.query(/*sql*/ `select current_user`);
    assertEquals(scalar, 'u_sha256');
  } finally {
    await conn1.end();
  }
});

test('write after end', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  await conn.end();
  await assertRejects(_ => conn.query(/*sql*/ `select`));
});

// test('write after end 2', async () => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   conn.end();
//   await new Promise(resolve => setImmediate(resolve, 0));
//   await assert.rejects(conn.query(/*sql*/ `SELECT`));
// });

test('pool should reuse connection', async _ => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
  try {
    const { scalar: pid1 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    const { scalar: pid2 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    assertEquals(pid1, pid2);
  } finally {
    await pool.end();
  }
});

// test('reset conn', async _ => {
//   const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
//   try {
//     await conn.query(/*sql*/ `set idle_in_transaction_session_timeout = -1 `);
//     await conn.query('begin; select 1').catch(Object);
//     // await conn.query('START TRANSACTION READ ONLY; rollback;');
//     // await delay(10000);
//     await conn.query(' rollback;');
//     // await conn.query(' create temp table hello as select 1;  select * from hello;')
//     // await conn.query('discard all;')
//   } finally {
//     await conn.end();
//   }
// });

test('pool should do connection per query when poolSize is unset', async () => {
  const pg = pgpool('postgres://postgres:secret@postgres:5432/postgres');
  const { scalar: pid } = await pg.query(/*sql*/ `select pg_backend_pid()`);
  assertEquals(typeof pid, 'number');
  const { scalar: terminated } = await pg.query(/*sql*/ `select pg_terminate_backend(${pid})`);
  assertEquals(terminated, false);
});

test('pool should prevent idle in trasaction', async _ => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
  try {
    // emit bad query with explicit transaction
    const q1 = Promise.resolve(pool.query(/*sql*/ `begin;`));
    q1.catch(Boolean);
    // then enqueue good innocent query in the same connection as previous query
    const q2 = Promise.resolve(pool.query(/*sql*/ `select 1;`));
    q2.catch(Boolean);

    await assertRejects(_ => q1, Error, 'pooled connection left in transaction');
    await assertRejects(_ => q2, Error, 'postgres query failed');

    // poisoned connection should be already destroyed and forgotten
    // in this event loop iteration and fresh connection should be created
    // so no errors expected
    const { scalar } = await pool.query(/*sql*/ `select 'hello'`);
    assertEquals(scalar, 'hello');
  } finally {
    await pool.end();
  }
});

// TODO it fails
test('pool should auto rollback', async _ => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
  try {
    // emit bad query with explicit transaction
    const q1 = Promise.resolve(pool.query(/*sql*/ `
      begin;
      create table this_table_should_not_be_created(a text);
    `));
    q1.catch(Boolean);

    // then enqueue `commit` in the same connection as previous query
    const q2 = Promise.resolve(pool.query(/*sql*/ `commit;`));
    q2.catch(Boolean);

    await assertRejects(_ => q1, Error, 'pooled connection left in transaction');
    await assertRejects(_ => q2, Error, 'postgres query failed');

    // if first query was not rollbacked then next query will fail
    await pool.query(/*sql*/ `
      create table this_table_should_not_be_created(a text);
      rollback;
    `);
  } finally {
    await pool.end();
  }
});

// test('pool - unexisting database', async () => {
//   const pool = pgwire.pool({
//     database: 'unicorn',
//   }, process.env.POSTGRES);
//   try {
//     await assert.rejects(pool.query(/*sql*/ `SELECT`), {
//       code: 'PGERR_3D000',
//     });
//   } finally {
//     pool.clear();
//   }
// });

// test('idle timeout', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES, {
//     idleTimeout: 200,
//   });
//   try {
//     await new Promise(resolve => conn.on('close', resolve));
//   } finally {
//     conn.end(); // close manually if fail
//   }
// });

// test('idle timeout 2', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES, {
//     idleTimeout: 200,
//   });
//   await Promise.all([
//     conn.query(/*sql*/ `SELECT`),
//     conn.query(/*sql*/ `SELECT`),
//     conn.query(/*sql*/ `SELECT`),
//   ]);
//   try {
//     await Promise.race([
//       new Promise(resolve => conn.on('close', resolve)),
//       new Promise((_, reject) => setTimeout(reject, 400, Error(
//         'Connection was not closed after idleTimeout',
//       ))),
//     ]);
//   } finally {
//     conn.end(); // close manually if fail
//   }
// });

// test('idle timeout 3', async () => {
//   const conn = await pgwire.connect(process.env.POSTGRES, {
//     idleTimeout: 200,
//   });
//   await conn.query(/*sql*/ `SELECT`);
//   await new Promise(resolve => setTimeout(resolve, 50));
//   await conn.query(/*sql*/ `SELECT`);
//   await new Promise(resolve => setTimeout(resolve, 50));
//   await conn.query(/*sql*/ `SELECT`);
//   try {
//     await Promise.race([
//       new Promise(resolve => conn.on('close', resolve)),
//       new Promise((_, reject) => setTimeout(reject, 400, Error(
//         'Connection was not closed after idleTimeout',
//       ))),
//     ]);
//   } finally {
//     conn.end();
//   }
// });

test('pool idle timeout', async () => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1&.poolIdleTimeout=1000');
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const { scalar: pid } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    const { scalar: alive } = await conn.query(/*sql*/ `
      select count(*) > 0 from pg_stat_activity where pid = '${pid}'
    `);
    assertEquals(alive, true);
    await delay(2000);
    const { scalar: stilAlive } = await conn.query(/*sql*/ `
      select count(*) > 0 from pg_stat_activity where pid = '${pid}'
    `);
    assertEquals(stilAlive, false);
  } finally {
    await conn.end();
    await pool.end();
  }
});

test('pool async error', async () => {
  const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
  try {
    const { scalar: pid1 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    await conn.query(/*sql*/ `select pg_terminate_backend('${pid1}')`);
    await delay(200);
    const { scalar: pid2 } = await pool.query(/*sql*/ `select pg_backend_pid()`);
    assertNotEquals(pid1, pid2);
  } finally {
    await pool.end();
    await conn.end();
  }
});

test('connection application_name', async () => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?application_name=test');
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT current_setting('application_name')`);
    assertEquals(scalar, 'test');
  } finally {
    await conn.end();
  }
});

// test('idleTimeout=0 should not close connection', async () => {
//   const conn = await pgwire.connect('postgres://postgres@postgres:5432/postgres');
//   try {
//     await new Promise(resolve => setTimeout(resolve, 200));
//     await conn.query(/*sql*/ `SELECT`);
//   } finally {
//     conn.end();
//   }
// });

test('unix socket', async () => {
  const conn = await pgwire.connect(process.env.POSTGRES_UNIX);
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT 'hello'`);
    assert.deepStrictEqual(scalar, 'hello');
  } finally {
    conn.end();
  }
});

test('copy to stdout', async _ => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  try {
    const stdout = conn.query(/*sql*/ `
      copy (values (1, 'hello'), (2, 'world')) to stdout
    `);
    const dump = await readAll(readerFromIterable(stdout));
    const dumps = new TextDecoder().decode(dump);
    assertEquals(dumps, '1\thello\n2\tworld\n');
  } finally {
    await conn.end();
  }
});

test('copy to stdout multi', async () => {
  const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
  try {
    const stdout = conn.query(/*sql*/ `
      copy (values (1, 'hello'), (2, 'world')) to stdout;
      copy (values (3, 'foo'), (4, 'bar')) to stdout;
    `);
    const dump = await readAll(readerFromIterable(stdout));
    const dumps = new TextDecoder().decode(dump);
    assertEquals(dumps, '1\thello\n2\tworld\n3\tfoo\n4\tbar\n');
  } finally {
    await conn.end();
  }
});

test('stream', async () => {
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

test('stream destroy', async () => {
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

test('cancel', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?replication=database&.debug=true');
  try {
    const response = conn.query(/*sql*/ `select pg_sleep(10)`);
    let startTime = Date.now();
    for await (const _ of response) {
      break;
    }
    const duration = Date.now() - startTime;
    if (duration > 1000) {
      throw Error(`cancel is too slow (${duration}ms), seems that cancel has no effect`);
    }
    // connection should be usable
    const { scalar } = await conn.query(/*sql*/ `select 'hello'`);
    assertEquals(scalar, 'hello');
  } finally {
    await conn.end();
  }
});


test('wake', async _ => {
  const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?.debug=0');
  console.log('-');
  try {
    const stream = conn.query(
      { statement: /*sql*/ `prepare q as select now() from pg_sleep(0.8)` },
      { statement: /*sql*/ `execute q;` },
      { message: 'Flush' },
      { statement: /*sql*/ `execute q;` },
      { message: 'Flush' },
      { statement: /*sql*/ `execute q;` },
      { message: 'Flush' },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { message: 'Flush' },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
      { statement: /*sql*/ `execute q;` },
    );
    let i = 0;
    for await (const chunk of stream) {
      console.log(chunk.tag, chunk.rows);

      if (i++ > 5) {
        conn._abortCtl.abort();
      }
    }
  } finally {
    await conn.end();
  }
});


test('first row', async _ => {
  const pg = await pgconnect('postgres://postgres@postgres:5432/postgres?.debug=1');
  try {
    const res = await pg.query(/*sql*/ `
      select 'hello', 'world';
      select 10, 'foo', 'bar', 'buz'
    `)
    console.log(res);
    const [a, b] = res;
    console.log(a, b);
  } finally {
    await pg.end();
  }
});

}
