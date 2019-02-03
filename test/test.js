const { deepStrictEqual } = require('assert');
const { Readable } = require('stream');
const { execSync } = require('child_process');
const pg = require('../lib/index.js');

it('wait for ready', async _ => {
  const conn = await pg.connectRetry()
  conn.end();
});

// it('connection url', async _ => {
//   const conn = await pg.connect();
//   const conn = await pg.connect('postgres://postgres@postgres:5432/postgres?a=1');
//   const conn = await pg.connect({
//     hostname: '127.0.0.1',
//     port: 5432,
//     user: 'postgres',
//     database: 'postgres',
//     application_name: 'hello',
//   });

//   const conn = await pg.connect({
//     ...pg.config,
//     application_name: 'hello',
//     replication: 'database',
//   });
// });

it('simple proto', async _ => {
  const result = await pg.query(/*sql*/ `SELECT 'hello'`);
  deepStrictEqual(result, {
    inTransaction: false,
    rows: [['hello']],
    empty: false,
    suspended: false,
    scalar: 'hello',
    command: 'SELECT 1',
    results: [{
      rows: [['hello']],
      command: 'SELECT 1',
    }],
  });
});

it('simple proto multi stmt', async _ => {
  const result = await pg.query(/*sql*/ `
    VALUES ('a'), ('b');
    VALUES ('c', 'd');
  `);
  deepStrictEqual(result, {
    inTransaction: false,
    results:[{
      rows: [['a'], ['b']],
      command: 'SELECT 2',
    }, {
      rows: [['c', 'd']],
      command: 'SELECT 1',
    }],
    rows: [[ 'c', 'd' ]],
    scalar: 'c',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('multiple queries', async _ => {
  const conn = await pg.connect();
  try {
    const [{ scalar: r1 }, { scalar: r2 }] = await Promise.all([
      conn.query(/*sql*/ `SELECT 'a'`),
      conn.query(/*sql*/ `SELECT 'b'`),
    ]);
    deepStrictEqual([r1, r2], ['a', 'b']);
  } finally {
    conn.end();
  }
});

it('extended proto', async _ => {
  const result = await (
    pg.queryExtended()
    .parse(/*sql*/ `SELECT 'hello'`)
    .bind()
    .execute()
    .fetch()
  );
  deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [['hello']],
      command: 'SELECT 1',
    }],
    rows: [['hello']],
    scalar: 'hello',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('portal suspended', async _ => {
  const result = await (
    pg.queryExtended()
    .parse(`SELECT 'hello' FROM generate_series(0, 10)`)
    .bind()
    .execute({ limit: 2 })
    .fetch()
  );
  deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [['hello'], ['hello']],
      suspended: true,
    }],
    rows: [['hello'], ['hello']],
    scalar: 'hello',
    command: undefined,
    empty: false,
    suspended: true,
  });
});

it('empty query', async _ => {
  const result = await (
    pg.queryExtended()
    .parse(``).bind().execute()
    .fetch()
  );
  deepStrictEqual(result, {
    inTransaction: false,
    results: [{
      rows: [],
      empty: true,
    }],
    rows: [],
    scalar: undefined,
    command: undefined,
    empty: true,
    suspended: false,
  });
});

it('simple copy in', async _ => {
  const { rows } = await pg.query({
    stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    sql: /*sql*/ `
      BEGIN;
      CREATE TABLE test(foo TEXT, bar TEXT);
      COPY test FROM STDIN;
      SELECT * FROM test;
    `,
  });
  deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
});

it('extended copy in', async _ => {
  const { rows } = await (
    pg.queryExtended()
    .parse(/*sql*/ `BEGIN`).bind().execute()
    .parse(/*sql*/ `CREATE TEMP TABLE test(foo TEXT, bar TEXT)`).bind().execute()
    .parse(/*sql*/ `COPY test FROM STDIN`).bind().execute({
      stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    })
    .parse(/*sql*/ `TABLE test`).bind().execute()
    .fetch()
  );
  deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
});

it('extended copy in missing 1', async _ => {
  const result = await (
    pg.queryExtended()
    .parse(/*sql*/ `BEGIN`).bind().execute()
    .parse(/*sql*/ `CREATE TEMP TABLE test(foo INT, bar TEXT)`).bind().execute()
    .parse(/*sql*/ `COPY test FROM STDIN`).bind().execute()
    .fetch()
    .catch(err => err.code)
  );
  deepStrictEqual(result, 'PGERR_57014');
});

it('simple copy in missing', async _ => {
  const result = await (
    pg.query({
      sql: /*sql*/ `
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
    })
    .catch(err => err.code)
  );
  deepStrictEqual(result, 'PGERR_57014');
});

it('row decode simple', async _ => {
  const { rows } = await pg.query(/*sql*/ `
    SELECT null,
      true, false,
      'hello'::text,
      '\\xdeadbeaf'::bytea,
      42::int2, -42::int2,
      42::int4, -42::int4,
      jsonb_build_object('hello', 'world', 'num', 1),
      json_build_object('hello', 'world', 'num', 1)
  `);
  deepStrictEqual(rows, [[
    null,
    true, false,
    'hello',
    Buffer.from('deadbeaf', 'hex'),
    42, -42,
    42, -42,
    { hello: 'world', num: 1 },
    { hello: 'world', num: 1 },
  ]]);
});

xit('array decode simple', async _ => {
  const { rows } = await pg.query(/*sql*/ `
    SELECT
      array[true, false],
      array[['a', 'b'], ['c', 'd']]
  `);
  deepStrictEqual(rows, [[
    [true, false],
    [['a', 'b'], ['c', 'd']],
  ]]);
});

it('row decode extended', async _ => {
  const { rows } = await (
    pg.queryExtended()
    .parse(/*sql*/ `
      SELECT null, true, false,
      'hello'::text,
      '\\xdeadbeaf'::bytea,
      42::int2, -42::int2,
      42::int4, -42::int4
    `)
    .bind()
    .execute()
    .fetch()
  );
  deepStrictEqual(rows, [[
    null, true, false,
    'hello',
    Buffer.from('deadbeaf', 'hex'),
    42, -42,
    42, -42,
  ]]);
});

it('bigint decode extended', async _ => {
  const { rows } = await (
    pg.queryExtended()
    .parse(/*sql*/ `SELECT 42::int8, -42::int8`)
    .bind()
    .execute()
    .fetch()
  );
  deepStrictEqual(rows, [[42n, -42n]]);
});

it('listen/notify', async _ => {
  const conn = await pg.connect();
  try {
    const payload = Promise.race([
      new Promise((_resolve, reject) => setTimeout(
        _ => reject(Error('no notification received in 1s')),
        1000
      )),
      new Promise((resolve, reject) => {
        conn.on('notify:test', ({ payload }) => resolve(payload));
        conn.on('error', reject);
      })
    ]);
    await conn.query(/*sql*/ `LISTEN test`);
    psql(/*sql*/ `NOTIFY test, 'hello'`);
    deepStrictEqual(await payload, 'hello');
  } finally {
    conn.end();
  }
});

it('logical replication', async _ => {
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
  deepStrictEqual(lines, expected);
});

it('CREATE_REPLICATION_SLOT issue', async _ => {
  // should not throw
  await pg.session(async conn => {
    await Promise.all([
      conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding'),
      conn.query('SELECT 1'),
    ]);
  }, { replication: 'database' });
});

it('logical replication pgoutput', async _ => {
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
  deepStrictEqual(pgomsgs, [{
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

it('logical replication ack', async _ => {
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
  deepStrictEqual(changesCount, 8);
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
  for await (const _ of replstream);
  const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
    SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  `));
  deepStrictEqual(changesCountAfterAck, 4);
});

it('param hint', async _ => {
  const { scalar } = await (
    pg.queryExtended()
    .parse({
      sql: /*sql*/ `SELECT $1`,
      paramTypes: ['int4'],
    })
    .bind({ params: [1] })
    .execute()
    .fetch()
  );
  deepStrictEqual(typeof scalar, 'number');
});

xit('connection session', async _ => {
  psql(/*sql*/ `CREATE SEQUENCE test_sess`);
  const conn = await pg.connect();
  try {
    const shouldBe1 = conn.session(async sess => {
      await new Promise(resolve => setTimeout(resolve, 100));
      return (
        sess.query(/*sql*/ `SELECT nextval('test_sess')::int`)
        .then(it => it.scalar)
      );
    });
    const shouldBe2 = (
      conn.query(/*sql*/ `SELECT nextval('test_sess')::int`)
      .then(it => it.scalar)
    );
    deepStrictEqual(
      await Promise.all([shouldBe1, shouldBe2]),
      [1, 2],
    );
  } finally {
    conn.end();
  }
});

function xit() {}
function it(name, fn) {
  it.tests = it.tests || [];
  it.tests.push({ name, fn });
}
async function main() {
  for (const { name, fn } of it.tests) {
    try {
      await fn();
      console.log(name, '- ok');
    } catch (err) {
      console.log(name, '- failed');
      throw err;
    }
  }
}
main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});

function arrstream(chunks) {
  return Readable({
    read() {
      chunks.forEach(x => this.push(x));
      this.push(null);
    }
  })
}

function psql(input) {
  return (
    execSync(`psql "${process.env.POSTGRES}" -Atc $'${input.replace(/'/g, '\\\'')}'`)
    .toString()
  );
}
