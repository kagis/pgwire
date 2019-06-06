const assert = require('assert');
const { Readable, pipeline, finished } = require('stream');
const { execSync } = require('child_process');
const fs = require('fs');
const { promisify } = require('util');
const pg = require('../lib/index.js');

const finishedp = promisify(finished);

it('wait for ready', async () => {
  const conn = await pg.connectRetry(process.env.POSTGRES);
  conn.end();
});

it('autoclose connections', async () => {
  const { scalar: pid } = await pg.query(/*sql*/ `SELECT pg_backend_pid()`);
  await new Promise(resolve => setTimeout(resolve, 100));
  const terminated = psql(/*sql*/ `SELECT pg_terminate_backend('${pid}')`).trim();
  assert.equal(terminated, 'f');
});

it('simple proto', async () => {
  const result = await pg.query(/*sql*/ `SELECT 'hello'`);
  assert.deepStrictEqual(result, {
    inTransaction: false,
    rows: [['hello']],
    empty: false,
    suspended: false,
    scalar: 'hello',
    command: 'SELECT 1',
    results: [{
      rows: [['hello']],
      command: 'SELECT 1',
      notices: [],
    }],
  });
});

it('simple proto multi stmt', async () => {
  const result = await pg.query(/*sql*/ `
    VALUES ('a'), ('b');
    VALUES ('c', 'd');
  `);
  assert.deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [['a'], ['b']],
      command: 'SELECT 2',
      notices: [],
    }, {
      rows: [['c', 'd']],
      command: 'SELECT 1',
      notices: [],
    }],
    rows: [['c', 'd']],
    scalar: 'c',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('multiple queries', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  try {
    const [{ scalar: r1 }, { scalar: r2 }] = await Promise.all([
      conn.query(/*sql*/ `SELECT 'a'`),
      conn.query(/*sql*/ `SELECT 'b'`),
    ]);
    assert.deepStrictEqual([r1, r2], ['a', 'b']);
  } finally {
    conn.end();
  }
});

it('extended proto', async () => {
  const result = await pg.query({
    extended: true,
    script: [{
      sql: /*sql*/ `SELECT 'hello'`,
    }],
  });
  assert.deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [['hello']],
      command: 'SELECT 1',
      notices: [],
    }],
    rows: [['hello']],
    scalar: 'hello',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('portal suspended', async () => {
  const result = await pg.query({
    extended: true,
    script: [{
      sql: /*sql*/ `SELECT 'hello' FROM generate_series(0, 10)`,
      limit: 2,
    }],
  });
  assert.deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [['hello'], ['hello']],
      suspended: true,
      notices: [],
    }],
    rows: [['hello'], ['hello']],
    scalar: 'hello',
    command: undefined,
    empty: false,
    suspended: true,
  });
});

it('empty query', async () => {
  const result = await pg.query({
    extended: true,
    script: [{
      sql: '',
    }],
  });
  assert.deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [],
      empty: true,
      notices: [],
    }],
    rows: [],
    scalar: undefined,
    command: undefined,
    empty: true,
    suspended: false,
  });
});

it('simple copy in', async () => {
  const { rows } = await pg.query({
    stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    script: /*sql*/ `
      BEGIN;
      CREATE TABLE test(foo TEXT, bar TEXT);
      COPY test FROM STDIN;
      SELECT * FROM test;
    `,
  });
  assert.deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
});

it('extended copy in', async () => {
  const { rows } = await pg.query({
    extended: true,
    script: [{
      sql: /*sql*/ `BEGIN`,
    }, {
      sql: /*sql*/ `CREATE TEMP TABLE test(foo TEXT, bar TEXT)`,
    }, {
      sql: /*sql*/ `COPY test FROM STDIN`,
      stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    }, {
      sql: /*sql*/ `TABLE test`,
    }],
  });
  assert.deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
});

it('extended copy in missing 1', async () => {
  const response = pg.query({
    extended: true,
    script: [
      { sql: /*sql*/ `BEGIN` },
      { sql: /*sql*/ `CREATE TEMP TABLE test(foo INT, bar TEXT)` },
      { sql: /*sql*/ `COPY test FROM STDIN` },
    ],
  });
  await assert.rejects(response, {
    code: 'PGERR_57014',
  });
});

it('simple copy in missing', async () => {
  const response = pg.query({
    script: /*sql*/ `
      BEGIN;
      CREATE TABLE test(foo INT, bar TEXT);
      COPY test FROM STDIN;
      SELECT 'hello';
      COPY test FROM STDIN;
      COPY test FROM STDIN;
    `,
    stdin: [
      arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
      arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
      // here should be third one
    ],
  });
  await assert.rejects(response, {
    code: 'PGERR_57014',
  });
});

it('row decode simple', async () => {
  const { rows } = await pg.query(/*sql*/ `
    SELECT null,
      true, false,
      'hello'::text,
      '\\xdeadbeaf'::bytea,
      42::int2, -42::int2,
      42::int4, -42::int4,
      42::int8, -42::int8,
      36.6::float4, -36.6::float4,
      36.6::float8, -36.6::float8,
      jsonb_build_object('hello', 'world', 'num', 1),
      json_build_object('hello', 'world', 'num', 1),
      '1/2'::pg_lsn,
      'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
      ARRAY[1, 2, 3]::int[],
      ARRAY[1, 2, 3]::text[],
      ARRAY['"quoted"', '{string}', '"{-,-}"'],
      ARRAY[[1, 2], [3, 4]],
      '[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[],
      ARRAY[1, NULL, 2]
  `);
  assert.deepStrictEqual(rows, [[
    null,
    true, false,
    'hello',
    Buffer.from('deadbeaf', 'hex'),
    42, -42,
    42, -42,
    BigInt(42), BigInt(-42),
    36.6, -36.6,
    36.6, -36.6,
    { hello: 'world', num: 1 },
    { hello: 'world', num: 1 },
    '00000001/00000002',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    [1, 2, 3],
    ['1', '2', '3'],
    ['"quoted"', '{string}', '"{-,-}"'],
    [[1, 2], [3, 4]],
    [[[1, 2, 3], [4, 5, 6]]],
    [1, null, 2],
  ]]);
});

it('row decode extended', async () => {
  const { rows } = await pg.query({
    extended: true,
    script: [{
      sql: /*sql*/ `
        SELECT null, true, false,
          'hello'::text,
          '\\xdeadbeaf'::bytea,
          42::int2, -42::int2,
          42::int4, -42::int4,
          42::int8, -42::int8,
          36.599998474121094::float4, -36.599998474121094::float4,
          36.6::float8, -36.6::float8,
          jsonb_build_object('hello', 'world', 'num', 1),
          json_build_object('hello', 'world', 'num', 1),
          '1/2'::pg_lsn,
          'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
          ARRAY[1, 2, 3]::int[],
          ARRAY[1, 2, 3]::text[],
          ARRAY['"quoted"', '{string}', '"{-,-}"'],
          ARRAY[[1, 2], [3, 4]],
          '[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[],
          ARRAY[1, NULL, 2]
      `,
    }],
  });
  assert.deepStrictEqual(rows, [[
    null, true, false,
    'hello',
    Buffer.from('deadbeaf', 'hex'),
    42, -42,
    42, -42,
    BigInt(42), BigInt(-42),
    36.599998474121094, -36.599998474121094,
    36.6, -36.6,
    { hello: 'world', num: 1 },
    { hello: 'world', num: 1 },
    '00000001/00000002',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    [1, 2, 3],
    ['1', '2', '3'],
    ['"quoted"', '{string}', '"{-,-}"'],
    [[1, 2], [3, 4]],
    [[[1, 2, 3], [4, 5, 6]]],
    [1, null, 2],
  ]]);
});

it('listen/notify', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  try {
    const response = new Promise((resolve, reject) => {
      conn.on('notify:test', ({ payload }) => resolve(payload));
      conn.on('error', reject);
    });
    await conn.query(/*sql*/ `LISTEN test`);
    psql(/*sql*/ `NOTIFY test, 'hello'`);
    assert.deepStrictEqual(await response, 'hello');
  } finally {
    conn.end();
  }
});

it('logical replication', async () => {
  psql(/*sql*/ `
    SELECT pg_create_logical_replication_slot('test', 'test_decoding');
    CREATE TABLE foo AS SELECT 1 a;
  `);
  const expected = JSON.parse(psql(/*sql*/ `
    SELECT json_agg(jsonb_build_object(
      'lsn', lpad(split_part(lsn::text, '/', 1), 8, '0') ||
        '/' || lpad(split_part(lsn::text, '/', 2), 8, '0'),
      'data', data
    ))
    FROM pg_logical_slot_peek_changes('test', NULL, NULL)
  `));
  const replstream = await pg.logicalReplication({
    slot: 'test',
    startLsn: '0/0',
  });
  const lines = [];
  const timer = setTimeout(_ => replstream.end(), 500);
  for await (const { lsn, data } of replstream) {
    lines.push({ lsn, data: data.toString() });
    timer.refresh();
  }
  assert.deepStrictEqual(lines, expected);
});

it('CREATE_REPLICATION_SLOT issue', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    replication: 'database',
  });
  try {
    await Promise.all([
      conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding'),
      conn.query(/*sql*/ `SELECT 1`),
    ]);
  } finally {
    conn.end();
  }
});

it('CREATE_REPLICATION_SLOT issue 1', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    replication: 'database',
  });
  try {
    await Promise.all([
      conn.query('CREATE_REPLICATION_SLOT crs_iss1 LOGICAL test_decoding'),
      assert.rejects(conn.query(/*sql*/ `SELECT 1/0`)),
      conn.query(/*sql*/ `SELECT 1`),
    ]);
  } finally {
    conn.end();
  }
});

it('logical replication pgoutput', async () => {
  psql(/*sql*/ `
    BEGIN;
    CREATE TABLE foo1(a INT NOT NULL PRIMARY KEY, b TEXT);
    -- ALTER TABLE foo1 REPLICA IDENTITY FULL;
    CREATE PUBLICATION pub1 FOR TABLE foo1;
    COMMIT;

    SELECT pg_create_logical_replication_slot('test1', 'pgoutput');

    BEGIN;
    INSERT INTO foo1 VALUES (1, 'hello'), (2, 'world');
    UPDATE foo1 SET b = 'all' WHERE a = 1;
    DELETE FROM foo1 WHERE a = 2;
    TRUNCATE foo1;
    COMMIT;
  `);
  const expectedRelation = {
    relationid: Number(psql(/*sql*/ `SELECT 'public.foo1'::regclass::oid`)),
    schema: 'public',
    name: 'foo1',
    replicaIdentity: 'd',
    attrs: [
      { flags: 1, name: 'a', typeid: 23, typemod: -1 },
      { flags: 0, name: 'b', typeid: 25, typemod: -1 },
    ],
  };
  const peekedChanges = JSON.parse(psql(/*sql*/ `
    SELECT json_agg(jsonb_build_object(
      'lsn', lpad(split_part(lsn::text, '/', 1), 8, '0') ||
        '/' || lpad(split_part(lsn::text, '/', 2), 8, '0'),
      'xid', xid::text::int
    ))
    FROM pg_logical_slot_peek_binary_changes('test1', NULL, NULL,
      'proto_version', '1', 'publication_names', 'pub1')
  `));
  const replstream = await pg.logicalReplication({
    slot: 'test1',
    startLsn: '0/0',
    options: {
      'proto_version': 1,
      'publication_names': 'pub1',
    },
  });
  const timer = setTimeout(_ => replstream.end(), 500);
  const pgomsgs = [];
  for await (const pgomsg of replstream.pgoutput()) {
    delete pgomsg.endLsn;
    delete pgomsg.time;
    delete pgomsg.finalLsn;
    delete pgomsg.commitTime;
    delete pgomsg.commitLsn;
    delete pgomsg._endLsn;
    pgomsgs.push(pgomsg);
    timer.refresh();
  }
  assert.deepStrictEqual(pgomsgs, [{
    tag: 'begin',
    xid: peekedChanges[0].xid,
    lsn: peekedChanges.shift().lsn,
  }, {
    tag: 'relation',
    lsn: (peekedChanges.shift().lsn, '00000000/00000000'), // why?
    ...expectedRelation,
  }, {
    tag: 'insert',
    lsn: peekedChanges.shift().lsn,
    after: { a: 1, b: 'hello' },
    relation: expectedRelation,
  }, {
    tag: 'insert',
    lsn: peekedChanges.shift().lsn,
    after: { a: 2, b: 'world' },
    relation: expectedRelation,
  }, {
    tag: 'update',
    lsn: peekedChanges.shift().lsn,
    relation: expectedRelation,
    before: null,
    after: { a: 1, b: 'all' },
  }, {
    tag: 'delete',
    lsn: peekedChanges.shift().lsn,
    relation: expectedRelation,
    keyOnly: true,
    before: { a: 2, b: null },
  }, {
    tag: 'relation',
    lsn: (peekedChanges.shift().lsn, '00000000/00000000'), // why?
    ...expectedRelation,
  }, {
    tag: 'truncate',
    lsn: peekedChanges.shift().lsn,
    cascade: false,
    restartSeqs: false,
    relations: [expectedRelation],
  }, {
    tag: 'commit',
    lsn: peekedChanges.shift().lsn,
    flags: 0,
  }]);
});

it('logical replication ack', async () => {
  psql(/*sql*/ `
    BEGIN;
    CREATE TABLE acktest(a INT NOT NULL PRIMARY KEY);
    CREATE PUBLICATION acktest FOR TABLE acktest;
    COMMIT;

    SELECT pg_create_logical_replication_slot('acktest', 'test_decoding');

    BEGIN;
    INSERT INTO acktest VALUES (1), (2);
    COMMIT;
    BEGIN;
    INSERT INTO acktest VALUES (3), (4);
    COMMIT;
  `);
  const changesCount = JSON.parse(psql(/*sql*/ `
    SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  `));
  assert.deepStrictEqual(changesCount, 8);
  const firstCommitLsn = JSON.parse(psql(/*sql*/ `
    SELECT to_json(lsn)
    FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
    WHERE data LIKE 'COMMIT%'
    LIMIT 1
  `));
  const replstream = await pg.logicalReplication({
    slot: 'acktest',
    startLsn: '0/0',
  });
  replstream.ack(firstCommitLsn);
  replstream.end();
  replstream.resume();
  await finishedp(replstream);
  const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
    SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  `));
  assert.deepStrictEqual(changesCountAfterAck, 4);
});

it('logical replication invalid startLsn', async () => {
  const response = pg.logicalReplication({
    slot: 'unicorn',
    startLsn: 'invalid_lsn',
  });
  await assert.rejects(response, {
    code: 'PGERR_INVALID_START_LSN',
  });
});

it('parse bind execute', async () => {
  const { scalar } = await pg.query({
    extended: true,
    script: [{
      op: 'parse',
      sql: /*sql*/ `SELECT $1`,
      paramTypes: ['int4'],
    }, {
      op: 'bind',
      params: [{
        type: 'int4',
        value: 1,
      }],
    }, {
      op: 'execute',
    }],
  });
  assert.deepStrictEqual(scalar, 1);
});

it('param explicit type', async () => {
  const { rows: [row] } = await pg.query({
    extended: true,
    script: [{
      sql: /*sql*/ `
        SELECT pg_typeof($1)::text, $1,
          pg_typeof($2)::text, $2,
          pg_typeof($3)::text, $3->>'key',
          pg_typeof($4)::text, $4::text,
          pg_typeof($5)::text, $5::text
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
        value: ['x', 'y', 'z'],
      }],
    }],
  });
  assert.deepStrictEqual(row, [
    'integer', 1,
    'boolean', true,
    'jsonb', 'hello',
    'text[]', '{1,2,3,NULL}',
    'bytea[]', '{"\\\\x78","\\\\x79","\\\\x7a"}',
  ]);
});

xit('connection session', async () => {
  psql(/*sql*/ `CREATE SEQUENCE test_sess`);
  const conn = await pg.connect(process.env.POSTGRES);
  try {
    const shouldBe1 = conn.session(async sess => {
      await new Promise(resolve => setTimeout(resolve, 100));
      return (
        sess.query(/*sql*/ `SELECT nextval('test_sess')::int`)
        .then(({ scalar }) => scalar)
      );
    });
    const shouldBe2 = (
      conn.query(/*sql*/ `SELECT nextval('test_sess')::int`)
      .then(({ scalar }) => scalar)
    );
    assert.deepStrictEqual(
      await Promise.all([shouldBe1, shouldBe2]),
      [1, 2],
    );
  } finally {
    conn.end();
  }
});

it('reject pending responses when connection close', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  try {
    await Promise.all([
      assert.rejects(conn.query(/*sql*/ `SELECT pg_terminate_backend(pg_backend_pid())`)),
      assert.rejects(conn.query(/*sql*/ `SELECT`)),
    ]);
  } finally {
    conn.end();
  }
});

it('notice', async () => {
  const { results } = await pg.query(/*sql*/ `ROLLBACK`);
  assert.deepStrictEqual(results[0].notices[0].code, '25P01');
});

it('pgbouncer', async () => {
  const pool = pg.pool(process.env.POSTGRES_PGBOUNCER);
  const { scalar } = await pool.query(/*sql*/ `SELECT 1`);
  assert.deepStrictEqual(scalar, 1);
});

it('auth clear text', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    user: 'u_clear',
    password: 'qwerty',
  });
  conn.end();
});

it('auth md5', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    user: 'u_md5',
    password: 'qwerty',
  });
  conn.end();
});

it('auth sha256', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    user: 'u_sha256',
    password: 'qwerty',
  });
  conn.end();
});

it('write after end', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  conn.end();
  await assert.rejects(conn.query(/*sql*/ `SELECT`));
});

it('write after end 2', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  conn.end();
  await new Promise(resolve => setImmediate(resolve, 0));
  await assert.rejects(conn.query(/*sql*/ `SELECT`));
});

it('pool - reset reused connection', async () => {
  const pool = pg.pool(process.env.POSTGRES, {
    // force all queries to execute in single connection
    poolMaxConnections: 1,
  });
  try {
    const { scalar: pid1 } = await pool.query(/*sql*/ `
      BEGIN;
      CREATE TABLE test(a TEXT);
      SELECT pg_backend_pid();
    `);
    // if previous query was not rollbacked then next query will fail
    const { scalar: pid2 } = await pool.query(/*sql*/ `
      BEGIN;
      CREATE TABLE test(a TEXT);
      SELECT pg_backend_pid();
    `);
    // ensure connection was reused
    assert.deepStrictEqual(pid1, pid2);
  } finally {
    pool.clear();
  }
});

it('pool - unexisting database', async () => {
  const pool = pg.pool(process.env.POSTGRES, {
    database: 'unicorn',
  });
  try {
    await assert.rejects(pool.query(/*sql*/ `SELECT`), {
      code: 'PGERR_3D000',
    });
  } finally {
    pool.clear();
  }
});

it('idle timeout', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
    idleTimeout: 200,
  });
  try {
    await new Promise(resolve => conn.on('close', resolve));
  } finally {
    conn.end(); // close manually if fail
  }
});

it('idle timeout 2', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
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

it('idle timeout 3', async () => {
  const conn = await pg.connect(process.env.POSTGRES, {
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

it('pool - idle timeout', async () => {
  const pool = pg.pool(process.env.POSTGRES, {
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

it('pool async error', async () => {
  const pool = pg.pool(process.env.POSTGRES, {
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

it('connection uri options', async () => {
  const conn = await pg.connect('postgres://postgres@postgres:5432/postgres?application_name=test');
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT current_setting('application_name')`);
    assert.equal(scalar, 'test');
  } finally {
    conn.end();
  }
});

it('idleTimeout=0 should not close connection', async () => {
  const conn = await pg.connect('postgres://postgres@postgres:5432/postgres');
  try {
    await new Promise(resolve => setTimeout(resolve, 200));
    await conn.query(/*sql*/ `SELECT`);
  } finally {
    conn.end();
  }
});

it('unix socket', async () => {
  const conn = await pg.connect(process.env.POSTGRES_UNIX);
  try {
    const { scalar } = await conn.query(/*sql*/ `SELECT 'hello'`);
    assert.deepStrictEqual(scalar, 'hello');
  } finally {
    conn.end();
  }
});

it('copy to file', async () => {
  const resp = pg.query(/*sql*/ `COPY (VALUES (1, 2)) TO STDOUT`);
  const fw = fs.createWriteStream('/tmp/test');
  await promisify(pipeline)(resp, fw);
  const content = fs.readFileSync('/tmp/test', { encoding: 'utf-8' });
  assert.deepStrictEqual(content, '1\t2\n');
});

it('copy to stdout', async () => {
  const resp = pg.query(/*sql*/ `COPY (VALUES (1, 2)) TO STDOUT`);
  const chunks = await readAllChunks(resp);
  assert.deepStrictEqual(String(Buffer.concat(chunks)), '1\t2\n');
});

it('copy to stdout 2', async () => {
  const resp = pg.query(/*sql*/ `
    COPY (VALUES (1, 2)) TO STDOUT;
    COPY (VALUES (3, 4)) TO STDOUT;
  `);
  const chunks = await readAllChunks(resp);
  assert.deepStrictEqual(String(Buffer.concat(chunks)), '1\t2\n3\t4\n');
});

it('stream', async () => {
  const resp = pg.query(/*sql*/ `SELECT 'hello' col`);
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

xit('stream destroy', async () => {
  const conn = await pg.connect(process.env.POSTGRES);
  try {
    const resp = conn.query(/*sql*/ `SELECT generate_series(0, 2000)`);
    // resp.resume();
    resp.destroy();
    const { scalar } = await conn.query(/*sql*/ `SELECT 'hello'`);
    assert.deepStrictEqual(scalar, 'hello');
  } finally {
    conn.end();
  }
});

// it('pool', async () => {
//   const pool = pg.pool(process.env.POSTGRES, {
//     poolMaxConnections: 4,
//     application_name: 'pgwire-pool-test',
//   });
//   for (let i = 0; i < 10; i++) {
//     pool.query(/*sql*/ `SELECT pg_sleep(.1)`);
//     await new Promise(resolve => setTimeout(resolve, 50));
//     console.log();
//   }
// });

function xit() {}
function it(name, fn) {
  it.tests = it.tests || [];
  it.tests.push({ name, fn });
}

async function main() {
  for (const { name, fn } of it.tests) {
    let testTimeout;
    try {
      await Promise.race([fn(), new Promise((_, reject) => {
        testTimeout = setTimeout(reject, 5000, Error('test timeout'));
      })]);
      // eslint-disable-next-line no-console
      console.log(name, '- ok');
    } catch (err) {
      // eslint-disable-next-line no-console
      console.log(name, '- failed', err);
      throw Error('test failed');
    } finally {
      clearTimeout(testTimeout);
    }
  }
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
  setTimeout(_ => {
    console.error('killing tests');
    process.exit(1);
  }, 1000).unref();
});

function arrstream(chunks) {
  return Readable({
    read() {
      for (const chunk of chunks) {
        this.push(chunk);
      }
      this.push(null);
    },
  });
}

function readAllChunks(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on('data', chunk => chunks.push(chunk));
    finished(readable, err => err ? reject(err) : resolve(chunks));
  });
}

function psql(input) {
  return (
    execSync(`psql "${process.env.POSTGRES}" -Atc $'${input.replace(/'/g, '\\\'')}'`)
    .toString()
  );
}
