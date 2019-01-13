const { deepStrictEqual } = require('assert');
const { Readable } = require('stream');
const { execSync } = require('child_process');
const { pgconnect } = require('../lib/connect.js');

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
  const conn = await pgconnect({
    replication: 'database',
  });
  const replstream = await conn.logicalReplication({
    slot: 'test',
    startLsn: '0/0',
  });
  const lines = [];
  const timer = setTimeout(_ => replstream.end(), 500);
  for await (const { lsn, data } of replstream) {
    lines.push({ lsn, data: data.toString() });
    timer.refresh();
  }
  conn.end();
  deepStrictEqual(lines, expected);
});

it('CREATE_REPLICATION_SLOT issue', async _ => {
  const conn = await pgconnect({
    replication: 'database',
  });
  await Promise.all([
    conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding'),
    conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding').catch(_ => null),
    conn.query('SELECT 1'),
    conn.end(),
  ]);
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
  const conn = await pgconnect({
    replication: 'database',
  });
  const replstream = await conn.logicalReplication({
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
  conn.end();
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
    after: ['1', 'hello'],
    relation: expectedRelation,
  }, {
    tag: 'insert',
    lsn: peekedChanges.shift().lsn,
    after: ['2', 'world'],
    relation: expectedRelation,
  }, {
    tag: 'update',
    lsn: peekedChanges.shift().lsn,
    relation: expectedRelation,
    before: null,
    after: ['1', 'all'],
  }, {
    tag: 'delete',
    lsn: peekedChanges.shift().lsn,
    relation: expectedRelation,
    keyOnly: true,
    before: ['2', null],
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

xit('logical replication ack'); // todo

it('simple proto', async _ => {
  const conn = await pgconnect();
  const response = conn.query(`SELECT 1`);
  conn.end();

  deepStrictEqual(await response, {
    inTransaction: false,
    rows: [['1']],
    empty: false,
    suspended: false,
    scalar: '1',
    command: 'SELECT 1',
    results: [{
      rows: [['1']],
      command: 'SELECT 1',
    }],
  });
});

it('simple proto multi stmt', async _ => {
  const conn = await pgconnect();
  const response = conn.query(`VALUES (1), (2); VALUES ('a', 'b');`);
  conn.end();
  deepStrictEqual(await response, {
    inTransaction: false,
    results:[{
      rows: [['1'], ['2']],
      command: 'SELECT 2',
    }, {
      rows: [['a', 'b']],
      command: 'SELECT 1',
    }],
    rows: [[ 'a', 'b' ]],
    scalar: 'a',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('extended proto', async _ => {
  const conn = await pgconnect();
  const response = (
    conn.extendedQuery()
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .fetch()
  );
  conn.end();
  deepStrictEqual(await response, {
    inTransaction: false,
    results: [{
      rows: [['1']],
      command: 'SELECT 1',
    }],
    rows: [['1']],
    scalar: '1',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  });
});

it('query queue', async _ => {
  const conn = await pgconnect();
  const response1 = (
    conn.extendedQuery()
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .fetch()
  );
  const response2 = (
    conn.extendedQuery()
    .parse(`SELECT 2`)
    .bind()
    .execute()
    .fetch()
  );
  conn.end();
  const result = await Promise.all([
    response1,
    response2,
  ]);

  deepStrictEqual(result, [{
    inTransaction: false,
    results: [{
      rows: [['1']],
      command: 'SELECT 1',
    }],
    rows: [['1']],
    scalar: '1',
    command: 'SELECT 1',
    empty: false,
    suspended: false,
  }, {
    inTransaction: false,
    results: [{
      rows: [['2']],
      command: 'SELECT 1',
    }],
    rows: [['2']],
    scalar: '2',
    command: 'SELECT 1',
    empty: false,
    suspended: false
  }]);
});

it('portal suspended', async _ => {
  const conn = await pgconnect();
  const response = (
    conn.extendedQuery()
    .parse(`SELECT generate_series(0, 10)`)
    .bind()
    .execute({ limit: 2 })
    .fetch()
  );
  conn.end();
  deepStrictEqual(await response, {
    inTransaction: false,
    results: [{
      rows: [['0'], ['1']],
      suspended: true,
    }],
    rows: [['0'], ['1']],
    scalar: '0',
    command: undefined,
    empty: false,
    suspended: true,
  });
});

it('empty query', async _ => {
  const conn = await pgconnect();
  const response = (
    conn.extendedQuery()
    .parse(``).bind().execute()
    .fetch()
  );
  conn.end();
  deepStrictEqual(await response, {
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

it('multiplexing', async _ => {
  const conn = await pgconnect();

  await (
    conn.extendedQuery()
    .parse('BEGIN').bind().execute()
    .parse({ name: 'stmt1', statement: `SELECT generate_series(10, 19)` })
    .bind({ name: 'stmt1', portal: 'portal1' })
    .parse({ name: 'stmt2', statement: `SELECT generate_series(20, 29)` })
    .bind({ name: 'stmt2', portal: 'portal2' })
    .fetch()
  );

  let some_portal_was_suspended;
  const elems = [];
  do {
    const { results: [first_result, second_result] } = await (
      conn.extendedQuery()
      .execute({ portal: 'portal1', limit: 2 })
      .execute({ portal: 'portal2', limit: 2 })
      .fetch()
    );
    elems.push(...first_result.rows.map(([it]) => it))
    elems.push(...second_result.rows.map(([it]) => it))
    some_portal_was_suspended = first_result.suspended || second_result.suspended;
  } while (some_portal_was_suspended);
  conn.end();

  deepStrictEqual(elems, [
    '10', '11', '20', '21',
    '12', '13', '22', '23',
    '14', '15', '24', '25',
    '16', '17', '26', '27',
    '18', '19', '28', '29',
  ]);
});

it('simple copy in', async _ => {
  const conn = await pgconnect();
  const response = conn.query({
    sql: /*sql*/ `
      BEGIN;
      CREATE TABLE test(foo INT, bar TEXT);
      COPY test FROM STDIN;
      SELECT * FROM test;
    `,
    stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
  });
  conn.end();
  const { rows } = await response;
  deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world']
  ]);
});

it('extended copy in', async _ => {
  const conn = await pgconnect();
  const response = (
    conn.extendedQuery()
    .parse('BEGIN').bind().execute()
    .parse('CREATE TEMP TABLE test(foo INT, bar TEXT)').bind().execute()
    .parse('COPY test FROM STDIN').bind().execute({
      stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    })
    .parse('SELECT * FROM test').bind().execute()
    .fetch()
  );
  conn.end();
  const { rows } = await response;
  deepStrictEqual(rows, [
    ['1', 'hello'],
    ['2', 'world'],
  ]);
});

it('extended copy in missing 1', async _ => {
  const conn = await pgconnect();
  const response = (
    conn.extendedQuery()
    .parse('BEGIN').bind().execute()
    .parse('CREATE TEMP TABLE test(foo INT, bar TEXT)').bind().execute()
    .parse('COPY test FROM STDIN').bind().execute()
    .fetch()
    .catch(err => err.code)
  );
  conn.end();
  deepStrictEqual(await response, 'PGERR_57014');
});

it('simple copy in missing', async _ => {
  const conn = await pgconnect();
  const response = conn.query({
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
  });
  conn.end();
  deepStrictEqual(await response.catch(err => err.code), 'PGERR_57014');
});

function xit() {}
function it(name, fn) {
  it.tests = it.tests || [];
  it.tests.push({ name, fn });
}
async function main() {
  for (const { name, fn } of it.tests) {
    await fn();
    console.log(name, '- ok');
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
