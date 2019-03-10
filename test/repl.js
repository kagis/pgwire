const net = require('net');
const repl = require('repl');
const { Writable } = require('stream');
const { BackendDecoder } = require('../lib/backend.js');
const fe = require('../lib/frontend.js');

const socket = net.connect({
  host: 'postgres', port: 5432,
  // host: 'pgbouncer', port: 6432,
});
const tx = new fe.FrontendEncoder();
tx.pipe(socket).pipe(new BackendDecoder()).pipe(new Writable({
  objectMode: true,
  write(message, _enc, done) {
    message.datas = String(message.data);
    // eslint-disable-next-line no-console
    console.log('->', JSON.stringify(message));
    return done();
  },
}));

socket.on('connect', () => {
  // eslint-disable-next-line no-console
  console.log('connected');
  const replServer = repl.start('');
  for (const m in fe) {
    replServer.context[m] = function (options) {
      tx.write(new fe[m](options));
    };
  }
  replServer.on('exit', () => {
    socket.end();
  });
  socket.on('close', () => {
    // eslint-disable-next-line no-console
    console.log('closed');
    process.exit(0);
  });
});
