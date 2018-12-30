const assert = require('assert');
const { pgconnect } = require('../lib/connect.js');

it('logical replication pgoutput', async _ => {

});

it('logical replication', async _ => {
  const conn = await pgconnect({
    replication: 'database',
  });

  await conn.query(`CREATE_REPLICATION_SLOT testslot LOGICAL test_decoding`).fetch();
  await conn.query(`CREATE TABLE hello AS SELECT 'hello' foo`);

  const repl_stream = (
    conn
    .query(`START_REPLICATION SLOT testslot LOGICAL 0/0`)
    .replication()
  );

  const changes = [];
  for await (const repl_msg of repl_stream) {
    const change = repl_msg.data.toString();
    if (/^COMMIT/.test(change)) {
      changes.push('COMMIT');
      break;
    } else if (/^BEGIN/.test(change)) {
      changes.push('BEGIN');
    } else {
      changes.push(change);
    }
  }
  conn.terminate();

  assert.deepEqual(changes, [
    `BEGIN`,
    `table public.hello: INSERT: foo[text]:'hello'`,
    `COMMIT`,
  ]);
});

it('simple proto', async _ => {
  const conn = await pgconnect();
  const result = await conn.query(`SELECT 1`).fetch();
  conn.terminate();

  assert.deepEqual(result, [{
    rows: [['1']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('simple proto multi stmt', async _ => {
  const conn = await pgconnect();
  const result = await conn.query(`VALUES (1), (2); VALUES ('a', 'b');`).fetch();
  conn.terminate();

  assert.deepEqual(result, [{
    rows: [['1'], ['2']],
    commandtag: 'SELECT 2',
    suspended: false,
    empty: false,
  }, {
    rows: [['a', 'b']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('extended proto', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .sync()
    .fetch()
  );
  conn.terminate();

  assert.deepEqual(result, [{
    rows: [['1']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('query queue', async _ => {
  const conn = await pgconnect();
  const cursor1 = await (
    conn
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .sync()
  );
  const cursor2 = await (
    conn
    .parse(`SELECT 2`)
    .bind()
    .execute()
    .sync()
  );
  const [result1, result2] = await Promise.all([
    cursor1.fetch(),
    cursor2.fetch(),
  ]);
  conn.terminate();

  assert.deepEqual(result1, [{
    rows: [['1']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
  assert.deepEqual(result2, [{
    rows: [['2']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('portal suspended', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn
    .parse(`SELECT generate_series(0, 10)`)
    .bind()
    .execute({ limit: 2 })
    .sync()
    .fetch()
  );
  conn.terminate();
  assert.deepEqual(result, [{
    rows: [['0'], ['1']],
    commandtag: undefined,
    suspended: true,
    empty: false,
  }]);
});

it('empty query', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn
    .parse(``)
    .bind()
    .execute()
    .sync()
    .fetch()
  );
  conn.terminate();
  assert.deepEqual(result, [{
    rows: [],
    commandtag: undefined,
    suspended: false,
    empty: true,
  }]);
});

it('multiplexing', async _ => {
  const conn = await pgconnect();
  conn.query('BEGIN').fetch();
  conn
  .parse({ name: 'stmt1', statement: `SELECT generate_series(10, 19)` })
  .bind({ name: 'stmt1', portal: 'portal1' })
  .parse({ name: 'stmt2', statement: `SELECT generate_series(20, 29)` })
  .bind({ name: 'stmt2', portal: 'portal2' });

  let some_portal_was_suspended;
  const elems = [];
  do {
    const [first_result, second_result] = await (
      conn
      .execute({ portal: 'portal1', limit: 2 })
      .execute({ portal: 'portal2', limit: 2 })
      .sync()
      .fetch()
    );
    elems.push(...first_result.rows.map(([it]) => it))
    elems.push(...second_result.rows.map(([it]) => it))
    some_portal_was_suspended = first_result.suspended || second_result.suspended;
  } while (some_portal_was_suspended);
  conn.terminate();

  assert.deepEqual(elems, [
    '10', '11', '20', '21',
    '12', '13', '22', '23',
    '14', '15', '24', '25',
    '16', '17', '26', '27',
    '18', '19', '28', '29',
  ]);
});


function it(testname, fn) {
  it.tests = it.tests || [];
  it.tests.push({ testname, fn });
}
async function main() {
  for (const { testname, fn } of it.tests) {
    await fn();
    console.log(testname, '- ok');
  }
}
main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
