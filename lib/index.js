const Pool = require('./pool.js');
const Connection = require('./connection.js');
const { pgliteral, pgident } = require('./escape.js');

module.exports = pool(
  process.env.POSTGRES ||
  'postgres://postgres@127.0.0.1:5432/postgres'
);

Object.assign(module.exports, {
  connect,
  connectRetry,
  pool,
  pgliteral,
  pgident,
});

async function connectRetry(url, options) {
  // TODO respect connect_timeout
  for (;;) {
    try {
      return await connect(url, options);
    } catch (err) {
      if (!(
        err.code == 'ENOTFOUND' ||
        err.code == 'ECONNREFUSED' ||
        err.code == 'ECONNRESET' ||
        err.code == 'PGERR_57P03' // cannot_connect_now
      )) {
        throw err;
      }
      await new Promise(resolve => setTimeout(resolve, 1e3));
    }
  }
}

function connect(url, options) {
  return new Promise((resolve, reject) => {
    const conn = new Connection({ ...url2conf(url), ...options });
    conn._connectIfNotConnected();
    conn.on('error', reject);
    conn.on('ready', _ => {
      conn.off('error', reject);
      resolve(conn);
    });
  });
}

function pool(url, options) {
  return new Pool({ ...url2conf(url), ...options });
}

function url2conf(urlstr) {
  const url = new URL(urlstr);
  return {
    hostname: url.hostname,
    port: url.port,
    user: url.username,
    password: url.password,
    database: url.pathname.replace(/^\//, '') || 'postgres',
    ...Array.from(url.searchParams).reduce(
      (obj, [key, val]) => (obj[key] = val, obj),
      {},
    ),
  };
}

// function configNormalize(optionsOrUrl) {
//   if (typeof optionsOrUrl == 'string') {
//     return url2conf(optionsOrUrl);
//   }
//   return optionsOrUrl;
// }
