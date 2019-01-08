const net = require('net');
const EventEmmiter = require('events');
const { Readable, Transform, finished, pipeline } = require('stream');
const { BackendDecoder } = require('./backend.js');
const fe = require('./frontend.js');
const { ReplicationStream } = require('./replication.js');

Object.assign(exports, {
  pgconnect,
});

// exports.JSONB_TYPE_OID = 3802;

async function pgconnect({
  url = process.env.POSTGRES || 'postgres://',
  retry_interval_s,
  ...options
} = {}) {
  url = new URL(url);
  options = {
    hostname: url.hostname || '127.0.0.1',
    port: url.port || 5432,
    user: url.username || 'postgres',
    password: url.password,
    database: url.pathname.replace(/^\//, '') || 'postgres',
    ...(
      Array.from(url.searchParams)
      .reduce((obj, [key, val]) => (obj[key] = val, obj), {})
    ),
    ...options,
  };

  for (;;) {
    try {
      return await new PGConnection()._connect(options);
    } catch (err) {
      if (!retry_interval_s || !(
        err.code == 'ENOTFOUND' ||
        err.code == 'ECONNREFUSED' ||
        err.code == 'ECONNRESET' ||
        err.code == 'PGERR_57P03' // cannot_connect_now
      )) {
        throw err;
      }
      console.error(
        `postgres is not available (${err.code}),`,
        `will try again after ${retry_interval_s}s`,
      );
      await new Promise(resolve => setTimeout(
        resolve,
        retry_interval_s * 1e3,
      ));
    }
  }
}

class PGConnection extends EventEmmiter {
  constructor() {
    super();
    this.parameters = {};
    this._desynced = false;
    this._sentMessages = [];
    this._blocking = false;
    this._blockedMessages = [];
    this._pendingResponses = [];
    this._response = null;
    this._stdinEncoder = null;
    this._socket = null;
    this._rx = null;
    this._tx = null;
  }

  async _connect({ hostname, port, password: _password, ...options }) {
    this._socket = net.connect({ host: hostname, port });
    this._socket.on('error', this._onError.bind(this));
    finished(this._socket, _ => {
      this.emit('terminate');
    });
    this._rx = new BackendDecoder();
    this._tx = new fe.FrontendEncoder();
    this._tx.pipe(this._socket).pipe(this._rx);
    this._rx.on('data', this._receiveMessage.bind(this));

    this._writeMessage(new fe.StartupMessage(options));
    for await (const _msg of this._getResponse()) {
      // console.log('connecting', _msg);
    }
    return this;
  }

  _writeMessage(outmsg) {
    if (this._blocking) {
      this._blockedMessages.push(outmsg);
      return;
    }
    if (this._desynced && !outmsg.sendWhenDesync) {
      return
    }
    this._tx.write(outmsg);
    // outmsg.emit('sent');
    if (outmsg instanceof fe.Terminate) {
      this._tx.end();
    }
    this._sentMessages.push(outmsg);
    if (outmsg.stdin) {
      this._blocking = true;
      outmsg.on('handled', _ => {
        this._blocking = false;
        this._tx.cork();
        while (!this._blocking && this._blockedMessages[0]) {
          this._writeMessage(this._blockedMessages.shift())
        }
        this._tx.uncork();
      });
    } else if (outmsg instanceof fe.Execute) {
      this._tx.write(new fe.CopyFail('PGERROR_MISSING_STDIN'));
    }
  }

  _onError(err) {
    if (this._response) {
      return setImmediate(_ => this._response.emit('error', err));
    }
    this.emit('error', err);
  }

  _receiveMessage(msg) {
    console.error('->', JSON.stringify(msg));
    switch (msg.tag) {
      case 'ReadyForQuery':
        this._sentMessages.shift().emit('handled');
        this._desynced = false;
        this._response.push(null);
        this._response = this._pendingResponses.shift();
        if (this._response) {
          this._activateResponse(this._response);
        }
        return;
      case 'ErrorResponse': {
        if (this._stdinEncoder) {
          this._stdinEncoder.destroy(new PgCopyError());
        }
        if (this._sentMessages[0].extendedQueryProto) {
          this._desynced = true;
          while (this._sentMessages[0] && !this._sentMessages[0].sendWhenDesync) {
            this._sentMessages.shift().emit('handled');
          }
        }
        const { tag: _, code, ...errorProps } = msg;
        return this._onError(Object.assign(new Error(), errorProps, {
          name: 'PGError',
          code: 'PGERR_' + code,
        }));
      }
      case 'RowDescription':
      case 'ParseComplete':
      case 'BindComplete':
      case 'CloseComplete':
      case 'CommandComplete':
      case 'EmptyQueryResponse':
      case 'PortalSuspended':
        if (this._sentMessages[0].extendedQueryProto) {
          this._sentMessages.shift().emit('handled');
        }
        break;
      case 'CopyInResponse':
      case 'CopyBothResponse': {
        const stdinFactory = this._sentMessages[0].stdin;
        if (!stdinFactory) {
          return console.log('missed copy')
        }
        const stdin = stdinFactory(msg);
        this._pipeCopyIn(stdin);
        break;
      }
      case 'NotificationResponse':
        this.emit('notify', msg);
        this.emit('notify:' + msg.channel, msg);
        return;
      case 'NoticeResponse':
        if (!this.emit('notice', msg)) {
          console.error(msg);
        }
        return;
      case 'ParameterStatus':
        this.parameters[msg.parameter] = msg.value;
        this.emit('parameter', msg.parameter, msg.value);
        return;
    }

    if (!this._response.push(msg)) {
      this._rx.pause();
    }
  }

  _pipeCopyIn(stdin) {
    this._stdinEncoder = new Transform({
      readableObjectMode: true,
      transform: (chunk, _enc, done) => done(null, new fe.CopyData(chunk)),
    });
    pipeline(stdin, this._stdinEncoder, err => {
      this._stdinEncoder = null;
      if (err instanceof PgCopyError) {
        return;
      }
      if (err) {
        this._tx.write(new fe.CopyFail(String(err)));
      } else {
        this._tx.write(new fe.CopyDone());
      }
      this._tx.write(new fe.Flush());
    });
    this._stdinEncoder.pipe(this._tx, { end: false });
  }

  query(optionsOrSql) {
    let { sql, stdin, closeConnection } = optionsOrSql;
    if (typeof optionsOrSql == 'string') {
      sql = optionsOrSql;
    }
    const msg = new fe.Query(sql);
    if (stdin) {
      msg.stdin = makeStdinMultiFactory(stdin);
    }
    this._writeMessage(msg);
    return this._getResponse();
  }

  extendedQuery() {
    return new ExtendedQuery(this);
  }

  terminate() {
    this._writeMessage(new fe.Terminate());
  }

  _getResponse() {
    const response = new Response();
    if (this._response) {
      this._pendingResponses.push(response);
    } else {
      this._response = response;
      this._activateResponse(response);
    }
    return response;
  }

  _activateResponse(response) {
    response.on('readrequest', _ => {
      this._rx.resume();
    });
  }
}

class PgCopyError extends Error {}

class Response extends Readable {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 1000,
    });
  }
  _read() {
    this.emit('readrequest');
  }
  fetch() {
    return new Promise((resolve, reject) => {
      let rows = [];
      const result = [];
      this.on('error', reject);
      this.on('end', _ => resolve(result));
      this.on('data', msg => {
        switch (msg.tag) {
          case 'DataRow':
            rows.push(msg.data);
            return;
          case 'CommandComplete':
          case 'PortalSuspended':
          case 'EmptyQueryResponse':
            result.push({
              rows,
              suspended: msg.tag == 'PortalSuspended',
              empty: msg.tag == 'EmptyQueryResponse',
              commandtag: msg.command_tag,
            });
            rows = [];
        }
      });
    });
  }
}

class ExtendedQuery {
  constructor(conn) {
    this._conn = conn;
    this._outMessages = [];
  }
  parse(options) {
    this._outMessages.push(new fe.Parse(options));
    return this;
  }
  describeStatement(statementName) {
    this._outMessages.push(new fe.DescribeStatement(statementName));
    return this;
  }
  bind(options) {
    this._outMessages.push(new fe.Bind(options));
    return this;
  }
  describePortal(portalName) {
    this._outMessages.push(new fe.DescribePortal(portalName));
    return this;
  }
  execute(options) {
    const msg = new fe.Execute(options);
    if (options && options.stdin) {
      msg.stdin = makeStdinFactory(options.stdin);
    }
    this._outMessages.push(msg);
    return this;
  }
  sync() {
    for (const outmsg of this._outMessages) {
      this._conn._writeMessage(outmsg);
    }
    this._conn._writeMessage(new fe.Sync());
    return this._conn._getResponse();
  }
  terminate() {
    for (const outmsg of this._outMessages) {
      this._conn._writeMessage(outmsg);
    }
    this._conn.terminate();
    return this._conn._getResponse();
  }
}

function makeStdinMultiFactory(arg) {
  if (Array.isArray(arg)) {
    const arr = arg.slice();
    return fmt => {
      const it = arr.shift();
      if (!it) {
        throw new Error('Too few stdins');
      }
      return makeStdinFactory(it)(fmt);
    };
  }
  return makeStdinFactory(arg);
}

function makeStdinFactory(arg) {
  if (typeof arg == 'function') {
    return arg;
  }
  if (Buffer.isBuffer(arg) || typeof arg == 'string') {
    return _ => new Readable({
      _read() {
        this.push(arg);
        this.push(null);
      },
    });
  }
  return _ => arg;
}
