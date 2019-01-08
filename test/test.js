const { deepEqual } = require('assert');
const { Readable } = require('stream');
const { pgconnect } = require('../lib/connect.js');

// it('logical replication pgoutput', async _ => {
//   const conn = await pgconnect();

//   // const stream = conn.query('COPY (VALUES(1,2)) TO STDOUT; COPY (VALUES(3,4)) TO STDOUT');
//   // for await (const msg of stream) {
//   //   console.log(msg);
//   // }
//   try {

//     const stream = conn._tx;
//     stream.write({
//       tag: 'Parse',
//       statement: 'select 1 / (10 - i) from generate_series(1, 10) i',
//     });
//     stream.write({
//       tag: 'Bind',
//     });
//     stream.write({
//       tag: 'Execute',
//       limit: 1,
//     });
//     stream.write({
//       tag: 'Query',
//       query: 'SELECT 1_',
//     });
//     // stream.write({
//     //   tag: 'Sync',
//     // });
//     // stream.end();

//     // const stream = conn.query(`
//     //   create table hello(id int);
//     //   -- select generate_series(0, 100);
//     //   -- COPY (VALUES(1,2)) TO STDOUT;
//     //   COPY hello from stdout;
//     //   COPY hello from stdout;
//     //   table hello;
//     // `);
//     // const stream = (
//     //   conn
//     //   .parse('create table hello(id int)').bind().execute()
//     //   .parse('COPY hello from stdout').bind().execute()
//     //   // .parse('COPY hello from stdout').bind().execute()
//     //   // .parse('table hello').bind().execute()
//     //   // .copydata('he\n')
//     //   // .copydata('1\n2\n')
//     //   // .copydone()
//     //   .sync()
//     // );
//     // stream.write();
//     // conn.copydata('hello\nhello\n');
//     // conn.copydata('1\n2\n');
//     // conn.copydone();
//     // conn.copydata('3\n4\n');
//     // conn.copydone();
//     // const cur = conn._cursor();
//     // // cur.on('error', console.log);
//     for await (const msg of conn._rx) {
//       console.log(msg);
//     }

//   } finally {
//     // conn.terminate();
//   }


//   // const [_parse, _bind, result] = await Promise.all([
//   //   conn.parse('create table'),
//   //   conn.bind(),
//   //   conn.execute(),
//   // ]);

//   // const repltx = new PassThrough();
//   // const replrx = await (
//   //   conn
//   //   .query({
//   //     sql: 'START_REPLICATION SLOT hello LOGICAL 0/0',
//   //     copyFrom: repltx,
//   //   })
//   // );

//   const replstream = await conn.startReplication('START_REPLICATION SLOT hello LOGICAL 0/0');
//   replstream.pipe(new PgOutputLogicalDecoder());
//   replstream.ack(lsn);

//   const response = conn.query({
//     sql: /*sql*/ `
//       CREATE TABLE tbl(foo int4);
//       COPY tbl FROM STDIN;
//       COPY tbl FROM STDIN;
//       SELECT * FROM tbl;
//     `,
//     copyFrom(descr) {

//     },
//     copyFrom: [
//       arrstream([
//         '1\n2\n',
//         '3\n4\n',
//       ]),
//     ],
//   });
//   const a = await response.fetch();


//   // conn.query([{
//   //   tag: 'parse',
//   // }, {
//   //   tag: 'bind'
//   // }])

//   const results = await (
//     conn.extendedQuery()
//     .parse('COPY hello FROM STDIN')
//     .bind()
//     .execute() // should throw unexpected_copyin
//     .parse('COPY tbl FROM STDIN')
//     .bind()
//     .execute({
//       // consume(output, results) {
//       //   // что если мне здесь нужен результат предыдущей команды ?
//       //   throw 'can throw - should terminate or skip until Z';
//       // },
//       copyFrom: arrstream([
//         '1\n2\n',
//         '3\n4\n',
//       ]),
//     })
//     .parse('SELECT 1')
//     .bind()
//     .execute()
//     .sync()
//   );

//   // for await (const result of results) {

//   // }



//   const { transaction_status } = await conn.sync();

//   process.exit(0);
// });

// it('logical replication', async _ => {
//   const conn = await pgconnect({
//     replication: 'database',
//   });

//   await conn.query(`CREATE_REPLICATION_SLOT testslot LOGICAL test_decoding`).fetch();
//   await conn.query(`CREATE TABLE hello AS SELECT 'hello' foo`);

//   const repl_stream = (
//     conn
//     .query(`START_REPLICATION SLOT testslot LOGICAL 0/0`)
//     .replication()
//   );

//   const changes = [];
//   for await (const repl_msg of repl_stream) {
//     const change = repl_msg.data.toString();
//     if (/^COMMIT/.test(change)) {
//       changes.push('COMMIT');
//       break;
//     } else if (/^BEGIN/.test(change)) {
//       changes.push('BEGIN');
//     } else {
//       changes.push(change);
//     }
//   }
//   conn.terminate();

//   deepEqual(changes, [
//     `BEGIN`,
//     `table public.hello: INSERT: foo[text]:'hello'`,
//     `COMMIT`,
//   ]);
// });

it('simple proto', async _ => {
  const conn = await pgconnect();
  const result = await conn.query(`SELECT 1`).fetch();
  conn.terminate();

  deepEqual(result, [{
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

  deepEqual(result, [{
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
    conn.extendedQuery()
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .sync()
    .fetch()
  );
  conn.terminate();

  deepEqual(result, [{
    rows: [['1']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('query queue', async _ => {
  const conn = await pgconnect();
  const response1 = (
    conn.extendedQuery()
    .parse(`SELECT 1`)
    .bind()
    .execute()
    .sync()
  );
  const response2 = (
    conn.extendedQuery()
    .parse(`SELECT 2`)
    .bind()
    .execute()
    .sync()
  );
  const [result1, result2] = await Promise.all([
    response1.fetch(),
    response2.fetch(),
  ]);
  conn.terminate();

  deepEqual(result1, [{
    rows: [['1']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
  deepEqual(result2, [{
    rows: [['2']],
    commandtag: 'SELECT 1',
    suspended: false,
    empty: false,
  }]);
});

it('portal suspended', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn.extendedQuery()
    .parse(`SELECT generate_series(0, 10)`)
    .bind()
    .execute({ limit: 2 })
    .sync()
    .fetch()
  );
  conn.terminate();
  deepEqual(result, [{
    rows: [['0'], ['1']],
    commandtag: undefined,
    suspended: true,
    empty: false,
  }]);
});

it('empty query', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn.extendedQuery()
    .parse(``)
    .bind()
    .execute()
    .sync()
    .fetch()
  );
  conn.terminate();
  deepEqual(result, [{
    rows: [],
    commandtag: undefined,
    suspended: false,
    empty: true,
  }]);
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
    .sync()
    .fetch()
  );

  let some_portal_was_suspended;
  const elems = [];
  do {
    const [first_result, second_result] = await (
      conn.extendedQuery()
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

  deepEqual(elems, [
    '10', '11', '20', '21',
    '12', '13', '22', '23',
    '14', '15', '24', '25',
    '16', '17', '26', '27',
    '18', '19', '28', '29',
  ]);
});

it('simple copy in', async _ => {
  const conn = await pgconnect();
  const result = await conn.query({
    sql: /*sql*/ `
      BEGIN;
      CREATE TABLE test(foo INT, bar TEXT);
      COPY test FROM STDIN;
      SELECT * FROM test;
    `,
    stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
  }).fetch();
  conn.terminate();
  deepEqual(result[3], {
    rows: [['1', 'hello'], ['2', 'world']],
    commandtag: 'SELECT 2',
    suspended: false,
    empty: false,
  });
});

it('extended copy in', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn.extendedQuery()
    .parse('BEGIN').bind().execute()
    .parse('CREATE TEMP TABLE test(foo INT, bar TEXT)').bind().execute()
    .parse('COPY test FROM STDIN').bind().execute({
      stdin: arrstream(['1\t', 'hello\n', '2\t', 'world\n']),
    })
    .parse('SELECT * FROM test').bind().execute()
    .sync()
    .fetch()
  );
  conn.terminate();
  deepEqual(result[3], {
    rows: [['1', 'hello'], ['2', 'world']],
    commandtag: 'SELECT 2',
    suspended: false,
    empty: false,
  });
});

it('extended copy in missing 1', async _ => {
  const conn = await pgconnect();
  const result = await (
    conn.extendedQuery()
    .parse('BEGIN').bind().execute()
    .parse('CREATE TEMP TABLE test(foo INT, bar TEXT)').bind().execute()
    .parse('COPY test FROM STDIN').bind().execute()
    .sync()
    .fetch()
    .catch(err => err.code)
  );
  conn.terminate();
  deepEqual(result, 'PGERROR_MISSING_STDIN');
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


function arrstream(chunks) {
  return Readable({
    read() {
      chunks.forEach(x => this.push(x));
      this.push(null);
    }
  })
}
