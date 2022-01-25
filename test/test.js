export function setup({
  pgconnect,
  pgpool,
  test,
  assertEquals,
}) {

  test('simple proto', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const res = await conn.query(/*sql*/ `
        do $$ begin raise notice 'test start'; end $$;
        select 'hello' a, 'world' b union all select 'bonjour', 'le monde';
        do $$ begin raise notice 'test end'; end $$;
      `);
      const [...row] = res;
      assertEquals(row, ['hello', 'world']);
      assertEquals({
        status: res.status,
        scalar: res.scalar,
        rows: res.rows,
        columns: res.columns,
        notices: res.notices?.map(it => ({ message: it?.message })),
        results: res.results?.map(it => ({
          status: it?.status,
          scalar: it?.scalar,
          rows: it?.rows,
          columns: it?.columns,
        })),
      }, {
        status: undefined,
        scalar: 'hello',
        rows: [['hello', 'world'], ['bonjour', 'le monde']],
        columns: [
          { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
          { binary: 0, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
        ],
        notices: [
          { message: 'test start' },
          { message: 'test end' },
        ],
        results: [{
          status: 'DO',
          scalar: undefined,
          rows: [],
          columns: [],
        }, {
          status: 'SELECT 2',
          scalar: 'hello',
          rows: [['hello', 'world'], ['bonjour', 'le monde']],
          columns: [
            { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
            { binary: 0, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
          ],
        }, {
          status: 'DO',
          scalar: undefined,
          rows: [],
          columns: [],
        }],
      });
    } finally {
      await conn.end();
    }
  });

  test('simple proto empty', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const res = await conn.query(/*sql*/ ``);
      const [...row] = res;
      assertEquals(row, []);
      assertEquals({
        status: res.status,
        scalar: res.scalar,
        rows: res.rows,
        columns: res.columns,
        notices: res.notices,
        results: res.results?.map(it => ({
          status: it?.status,
          scalar: it?.scalar,
          rows: it?.rows,
          columns: it?.columns,
        })),
      }, {
        status: 'EmptyQueryResponse',
        scalar: undefined,
        rows: [],
        columns: [],
        notices: [],
        results: [{
          status: 'EmptyQueryResponse',
          scalar: undefined,
          rows: [],
          columns: [],
        }],
      });
    } finally {
      await conn.end();
    }
  });

  test('extended proto', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const res = await conn.query({
        statement: /*sql*/ `do $$ begin raise notice 'test start'; end $$`
      }, {
        // suspended
        statement: /*sql*/ `select 'test' a from generate_series(0, 10) i`,
        limit: 1,
      }, {
        // binary output
        statement: /*sql*/ `select 16909060::int4 a, 16909060::int4 b, decode('0001020304', 'hex') c`,
        binary: [false, true, true],
      }, {
        // binary params
        statement: /*sql*/ `select $1 a, $2 b`,
        params: [
          { type: 'int4', value: Uint8Array.of(1, 2, 3, 4) },
          { type: 'bytea', value: Uint8Array.of(0, 1, 2, 3, 4) },
        ],
      }, {
        statement: /*sql*/ `select $1 a, 'world' b union all select $2, 'le monde'`,
        params: [{ type: 'text', value: 'hello' }, { type: 'text', value: 'bonjour' }],
      }, {
        statement: /*sql*/ `do $$ begin raise notice 'test end'; end $$;`
      }, {
        statement: /*sql*/ `/*empty*/`
      });

      const [...row] = res;
      assertEquals(row, ['hello', 'world']);
      assertEquals({
        status: res.status,
        scalar: res.scalar,
        rows: res.rows,
        columns: res.columns,
        notices: res.notices?.map(it => ({ message: it?.message })),
        results: res.results?.map(it => ({
          status: it?.status,
          scalar: it?.scalar,
          rows: it?.rows,
          columns: it?.columns,
        })),
      }, {
        status: undefined,
        scalar: 'hello',
        rows: [['hello', 'world'], ['bonjour', 'le monde']],
        columns: [
          { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
          { binary: 0, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
        ],
        notices: [
          { message: 'test start' },
          { message: 'test end' },
        ],
        results: [{
          status: 'DO',
          scalar: undefined,
          rows: [],
          columns: [],
        }, {
          status: 'PortalSuspended',
          scalar: 'test',
          rows: [['test']],
          columns: [{ binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 }],
        }, {
          status: 'SELECT 1',
          scalar: 16909060,
          rows: [[16909060, Uint8Array.of(1, 2, 3, 4), Uint8Array.of(0, 1, 2, 3, 4)]],
          columns: [
            { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 },
            { binary: 1, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 },
            { binary: 1, name: 'c', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 17, typeSize: 65535 }
          ],
        }, {
          status: 'SELECT 1',
          scalar: 16909060,
          rows: [[16909060, Uint8Array.of(0, 1, 2, 3, 4)]],
          columns: [
            { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 },
            { binary: 0, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 17, typeSize: 65535 }
          ],
        }, {
          status: 'SELECT 2',
          scalar: 'hello',
          rows: [['hello', 'world'], ['bonjour', 'le monde']],
          columns: [
            { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
            { binary: 0, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: 65535 },
          ],
        }, {
          status: 'DO',
          scalar: undefined,
          rows: [],
          columns: [],
        }, {
          status: 'EmptyQueryResponse',
          scalar: undefined,
          rows: [],
          columns: [],
        }],
      });
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

  test('connect with unexisting user', async _ => {
    const [q] = await Promise.allSettled([
      pgconnect('postgres://unknown@postgres:5432/postgres'),
    ]);
    assertEquals(q.status, 'rejected');
    if (!(q.reason?.name == 'PgError.28000')) throw q.reason;
  });

  test('copy from stdin extended', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const datasrc = ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode);
      const { rows } = await conn.query(
        { statement: /*sql*/ `create temp table test(foo text, bar text)` },
        { statement: /*sql*/ `copy test from stdin`, stdin: datasrc },
        { statement: /*sql*/ `select * from test` },
      );
      assertEquals(rows, [['1', 'hello'], ['2', 'world']]);
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
  });

  test('copy from stdin extended missing', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const [q] = await Promise.allSettled([conn.query(
        { statement: /*sql*/ `create temp table test(foo int, bar text)` },
        { statement: /*sql*/ `copy test from stdin` },
      )]);
      assertEquals(q.status, 'rejected');
      if (!(q.reason?.name == 'PgError.57014')) throw q.reason;
    } finally {
      await conn.end();
    }
  });

  test('copy from stdin simple', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const { rows } = await conn.query(/*sql*/ `
        create temp table test(foo text, bar text);
        copy test from stdin;
        select * from test;
      `, {
        stdin: ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
      });
      assertEquals(rows, [['1', 'hello'], ['2', 'world']]);
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
  });

  test('copy from stdin simple 2', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const { rows } = await conn.query(/*sql*/ `
        create temp table test(foo text, bar text);
        copy test from stdin;
        copy test from stdin;
        select * from test;
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
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
  });

  test('copy from stdin simple missing', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const [q] = await Promise.allSettled([conn.query(/*sql*/ `
        create temp table test(foo int, bar text);
        copy test from stdin;
        select 'hello';
        copy test from stdin;
        copy test from stdin;
      `, {
        stdins: [
          ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
          ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
          // here should be third one
        ],
      })]);
      assertEquals(q.status, 'rejected');
      if (!(q.reason?.name == 'PgError.57014')) throw q.reason;
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
  });

  test('copy to stdout', async _ => {
    const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
    try {
      const stdout = conn.query(/*sql*/ `copy (values (1, 'hello'), (2, 'world')) to stdout`);
      const dump = await readAll(stdout)
      const dumps = new TextDecoder().decode(dump);
      assertEquals(dumps, '1\thello\n2\tworld\n');
    } finally {
      await conn.end();
    }
  });

  test('copy to stdout multi', async _ => {
    const conn = await pgconnect('postgres://postgres:qwerty@postgres:5432/postgres');
    try {
      const stdout = conn.query(/*sql*/ `
        copy (values (1, 'hello'), (2, 'world')) to stdout;
        copy (values (3, 'foo'), (4, 'bar')) to stdout;
      `);
      const dump = await readAll(stdout);
      const dumps = new TextDecoder().decode(dump);
      assertEquals(dumps, '1\thello\n2\tworld\n3\tfoo\n4\tbar\n');
    } finally {
      await conn.end();
    }
  });

  async function readAll(iter) {
    const chunks = [];
    let nbytes = 0;
    for await (const chunk of iter) {
      assertEquals(chunk instanceof Uint8Array, true);
      chunks.push(chunk);
      nbytes += chunk.byteLength;
    }
    const result = new Uint8Array(nbytes);
    let pos = 0;
    for (const chunk of chunks) {
      result.set(chunk, pos);
      pos += chunk.byteLength;
    }
    return result;
  }

  test('type decode', async _ => {
    const statement = String.raw /*sql*/ `
      select null,
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
    `;
    const expected = [
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
    ]
    const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
    try {
      const [[...simple], [...ext]] = await Promise.all([
        conn.query(statement),
        conn.query({ statement }),
      ]);
      assertEquals(simple, expected);
      assertEquals(ext, expected);
    } finally {
      await conn.end();
    }
  });

  test('type encode', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      const [...row] = await conn.query({
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
      await conn.end();
    }
  });

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

  _test('logical replication', async _ => {
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

  test('logical replication pgoutput', async _ => {
    const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres?replication=database');
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
      const [hello] = await conn.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await conn.end();
    }

    function noproto(obj) {
      return Object.assign(Object.create(null), obj);
    }
  });


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

  _test('parse bind execute', async _ => {
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

  _test('reject pending responses when connection close', async _ => {
    let caughtError;
    const conn = await pgconnect('postgres://postgres:secret@postgres:5432/postgres');
    try {
      await Promise.all([
        conn.query(/*sql*/ `select pg_terminate_backend(pg_backend_pid())`).catch(Boolean),
        conn.query(/*sql*/ `select`),
      ]);
    } catch (err) {
      caughtError = err;
    } finally {
      await conn.end();
    }
    if (!caughtError) throw Error('Error expected');
    // TODO find a way to set and get client error kind
    if (!(caughtError.message == 'ololo')) throw caughtError;
  });


// test('pgbouncer', async () => {
//   const pool = pgwire.pool(process.env.POSTGRES_PGBOUNCER);
//   const { scalar } = await pool.query(/*sql*/ `SELECT 1`);
//   assert.deepStrictEqual(scalar, 1);
// });

  test('auth clear', async _ => {
    const [q] = await Promise.allSettled([
      pgconnect('postgres://pwduser@postgres:5432/postgres'),
    ]);
    assertEquals(q.status, 'rejected');
    if (!/password required \(clear\)/.test(q.reason.message)) throw q.reason;

    const conn = await pgconnect('postgres://pwduser:secret@postgres:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'pwduser');
    } finally {
      await conn.end();
    }
  });

  test('auth md5', async _ => {
    const [q] = await Promise.allSettled([
      pgconnect('postgres://md5user@postgres:5432/postgres'),
    ]);
    assertEquals(q.status, 'rejected');
    if (!/password required \(md5\)/.test(q.reason.message)) throw q.reason;

    const conn = await pgconnect('postgres://md5user:secret@postgres:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'md5user');
    } finally {
      await conn.end();
    }
  });

  test('auth scram-sha-256', async _ => {
    const [q] = await Promise.allSettled([
      pgconnect('postgres://sha256user@postgres:5432/postgres'),
    ]);
    assertEquals(q.status, 'rejected');
    if (!/password required \(scram-sha-256\)/.test(q.reason.message)) throw q.reason;

    const conn = await pgconnect('postgres://sha256user:secret@postgres:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'sha256user');
    } finally {
      await conn.end();
    }
  });

  test('write after end', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    await conn.end();
    const [q] = await Promise.allSettled([
      conn.query(/*sql*/ `select`),
    ]);
    assertEquals(q.status, 'rejected');
    if (!(q.reason?.message == 'connection destroyed')) throw q.reason;
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
      const [pid1] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      const [pid2] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(pid1, pid2);
    } finally {
      await pool.end();
    }
  });

  test('pool should do connection per query when poolSize is unset', async _ => {
    const pool = pgpool('postgres://postgres@postgres:5432/postgres');
    try {
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(typeof pid, 'number');
      const [terminated] = await pool.query(/*sql*/ `select pg_terminate_backend(${pid})`);
      assertEquals(terminated, false);
    } finally {
      await pool.end();
    }
  });

  test('pool should prevent idle in trasaction', async _ => {
    const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
    try {
      const [q1, q2] = await Promise.allSettled([
        // emit bad query with explicit transaction
        pool.query(/*sql*/ `begin;`),
        // then enqueue good innocent query in the same connection as previous query
        pool.query(/*sql*/ `select 1;`)
      ]);

      assertEquals(q1.status, 'rejected');
      assertEquals(q1.reason.message, 'pooled connection left in transaction');
      assertEquals(q2.status, 'rejected');
      assertEquals(q2.reason.message, 'postgres query failed')

      // poisoned connection should be already destroyed and forgotten
      // in this event loop iteration and fresh connection should be created
      // so no errors expected
      const [hello] = await pool.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await pool.end();
    }
  });

  // TODO it fails
  _test('pool should auto rollback', async _ => {
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

  test('pool idle timeout', async _ => {
    const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1&.poolIdleTimeout=1000');
    try {
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(typeof pid, 'number');
      const [alive] = await pool.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(alive, true);
      await delay(2000);
      const [stilAlive] = await pool.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(stilAlive, false);
    } finally {
      await pool.end();
    }
  });

  test('pool async error', async _ => {
    // use pool with single connection
    const pool = pgpool('postgres://postgres@postgres:5432/postgres?.poolSize=1');
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
    try {
      // new pooled connection should be created
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      const [alive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(alive, true);
      // cause pooled connection down,
      await conn.query(/*sql*/ `select pg_terminate_backend(${pid})`);
      await delay(200);
      const [stilAlive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(stilAlive, false);
      // pool should be able to execute queries in new connection
      const [hello] = await pool.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await pool.end();
      await conn.end();
    }
  });

  test('connection application_name', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?application_name=test');
    try {
      const [appname] = await conn.query(/*sql*/ `show application_name`);
      assertEquals(appname, 'test');
    } finally {
      await conn.end();
    }
  });

  _test('unix socket', async _ => {
    const conn = await pgwire.connect(process.env.POSTGRES_UNIX);
    try {
      const { scalar } = await conn.query(/*sql*/ `select 'hello'`);
      assert.deepStrictEqual(scalar, 'hello');
    } finally {
      conn.end();
    }
  });

  _test('stream', async _ => {
    const client = pgwire.pool(process.env.POSTGRES);
    const resp = client.query(/*sql*/ `select 'hello' col`);
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

  test('cancel', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?replication=database');
    try {
      const response = conn.query(/*sql*/ `select pg_sleep(10)`);
      const startTime = Date.now();
      for await (const _ of response) {
        break;
      }
      const duration = Date.now() - startTime;
      if (duration > 1000) {
        throw Error(`cancel is too slow (${duration}ms), seems that cancel has no effect`);
      }
      // connection should be usable
      const [hello] = await conn.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await conn.end();
    }
  });

  _test('wake', async _ => {
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

  _test('notifications', async _ => {
    const conn = await pgconnect('postgres://postgres@postgres:5432/postgres?.debug=1');
    try {
      const abortctl = new AbortController();
      const timeout = setTimeout(_ => abortctl.abort(), 10000);
      let n;
      await conn.query(/*sql*/ `listen test_chan`);
      const emitterTask = runEmitter();
      for await (n of conn.notifications({ signal: abortctl.signal })) {
        console.log(n);
        await conn.query(/*sql*/ `select 'hello'`);

        // break;
        // если мы тут начнем делать query,
        // то query повиснут если в это время в соединение придет NotificationResponse.
        // recvMessages застрянет на ожидании доставки NotificationResponse.
        // а этот цикл застрянет на ожидании ответа на запрос.
        // TODO How to make backpreassured notification subscribtion and avoid deadlock for queries?
      }
      clearTimeout(timeout);
      // console.log(n);
      await emitterTask;
    } finally {
      await conn.end();
    }

    async function runEmitter() {
      const conn = await pgconnect('postgres://postgres@postgres:5432/postgres');
      try {
        await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello1')`);
        await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello2')`);
        await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello3')`);
        await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello4')`);
        await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello5')`);
      } finally {
        await conn.end();
      }
    }
  });

}

// mute
function _test() {}

async function delay(duration) {
  return new Promise(resolve => setTimeout(resolve, duration));
}
