const pgwire = require('../lib/index.js');

async function run() {

  const conn = await pgwire.connectRetry(process.env.POSTGRES);
  conn.end();

  const pool = pgwire.pool({ poolMaxConnections: 5 }, process.env.POSTGRES);
  try {

    // prewarm pool
    await pool.query(`SELECT 1`);

    // eslint-disable-next-line no-console
    console.time('pool');
    await Promise.all(
      Array(10).fill(0)
      .map(_ => pool.query(`SELECT pg_sleep(1)`)),
    );
    // eslint-disable-next-line no-console
    console.timeEnd('pool');
  } finally {
    pool.clear();
  }

}

run().catch(err => setImmediate(_ => {
  throw err;
}));
