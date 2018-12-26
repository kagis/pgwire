const assert = require('assert');
const { pgconnect } = require('../lib/connect.js');

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