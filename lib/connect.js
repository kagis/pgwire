const net = require('net');
const EventEmmiter = require('events');
const { Readable, finished } = require('stream');
const { BackendDecoder } = require('./backend.js');
const fe = require('./frontend.js');
const { logicalReplication } = require('./replication.js');

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
    user: url.username,
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
      return await new Connection()._connect(options);
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

class Connection extends EventEmmiter {
  constructor() {
    super();
    this.parameters = {};
    this._pendingResponses = [];
    this._response = null;
    this._socket = null;
    this._rx = null;
    this._tx = null;
  }
  query(optionsOrSql) {
    return fetchResponse(this.queryStream(optionsOrSql));
  }
  queryStream(optionsOrSql) {
    let { sql, stdin } = optionsOrSql;
    if (typeof optionsOrSql == 'string') {
      sql = optionsOrSql;
    }
    this._tx.write(new fe.Query(sql));
    if (stdin) {
      for (const it of [].concat(stdin)) {
        this._tx.write(new fe._CopyUpstream(it));
      }
    }
    const response = this._addResponse();
    if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(sql)) {
      // should wait for ReadyForQuery
      this._tx.write(new fe._Block(new Promise(
        resolve => response.on('readyForQuery', resolve)
      )));
    } else {
      this._tx.write(new fe.CopyFail('Missing copy upstream'));
    }
    return response;
  }
  extendedQuery() {
    return new ExtendedQuery(this);
  }
  logicalReplication(options) {
    return logicalReplication(this, options);
  }
  end() {
    this._tx.write(new fe.Terminate());
    this._tx.end();
  }

  _connect({ hostname, port, password: _password, ...options }) {
    this._socket = net.connect({ host: hostname, port });
    this._socket.on('error', this._onSocketError.bind(this));
    finished(this._socket, err => {
      this.emit('terminate', err);
    });
    this._rx = new BackendDecoder();
    this._tx = new fe.FrontendEncoder();
    this._tx.pipe(this._socket).pipe(this._rx);
    this._rx.on('data', this._recvMessage.bind(this));
    this._tx.write(new fe.StartupMessage(options));
    return fetchResponse(this._addResponse()).then(_ => this);
  }
  _recvMessage(msg) {
    // console.error('->', JSON.stringify(msg));
    switch (msg.tag) {
      case 'NotificationResponse': return this._recvNotification(msg);
      case 'NoticeResponse': return this._recvNotice(msg);
      case 'ParameterStatus': return this._recvParameterStatus(msg);
    }
    if (!this._response.push(msg)) {
      this._rx.pause();
    }
    switch (msg.tag) {
      case 'ReadyForQuery': return this._recvReadyForQuery(msg);
      case 'ErrorResponse': return this._recvError(msg);
    }
  }
  _recvReadyForQuery(msg) {
    const response = this._response;
    response.emit('readyForQuery', msg);
    if (response._pgerror) {
      // raise error after last 'data' event handled
      setImmediate(_ => response.emit('error', response._pgerror));
    } else {
      response.push(null);
    }
    this._response = this._pendingResponses.shift();
    if (this._response) {
      this._activateResponse(this._response);
    }
  }
  _recvError({ tag: _, code, ...errorProps }) {
    const err = Object.assign(new Error(), errorProps, {
      name: 'PGError',
      code: 'PGERR_' + code,
    });
    if (!this._response) {
      return this.emit('error', err);
    }
    this._response.emit('errorResponse', err);
    this._response._pgerror = Object.assign(err, {
      prevError: this._response._pgerror,
    });
  }
  _recvNotification(msg) {
    this.emit('notify', msg);
    this.emit('notify:' + msg.channel, msg);
  }
  _recvParameterStatus(msg) {
    this.parameters[msg.parameter] = msg.value;
    this.emit('parameter', msg.parameter, msg.value);
  }
  _recvNotice(msg) {
    if (this._response && this._response.emit('notice', msg)) {
      return;
    }
    if (this.emit('notice', msg)) {
      return;
    }
    console.error(msg);
  }
  _onSocketError(err) {
    if (this._response) {
      return this._response.emit('error', err);
    }
    this.emit('error', err);
  }
  _addResponse() {
    const response = new Readable({
      objectMode: true,
      read() {
        this.emit('readrequest');
      },
    });
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

class ExtendedQuery {
  constructor(conn) {
    this._conn = conn;
    this._messages = [];
  }
  parse(options) {
    this._messages.push(new fe.Parse(options));
    return this;
  }
  describeStatement(statementName) {
    this._messages.push(new fe.DescribeStatement(statementName));
    return this;
  }
  bind(options) {
    this._messages.push(new fe.Bind(options));
    return this;
  }
  describePortal(portalName) {
    this._messages.push(new fe.DescribePortal(portalName));
    return this;
  }
  execute(options) {
    this._messages.push(new fe.Execute(options));
    if (options && options.stdin) {
      this._messages.push(new fe._CopyUpstream(options.stdin));
    } else {
      this._messages.push(new fe.CopyFail('Missing copy upstream'));
    }
    return this;
  }
  stream() {
    for (const m of this._messages) {
      this._conn._tx.write(m);
    }
    this._conn._tx.write(new fe.Sync());
    return this._conn._addResponse();
  }
  fetch() {
    return fetchResponse(this.stream());
  }
}

function fetchResponse(stream) {
  return new Promise((resolve, reject) => {
    let rows = [];
    let inTransaction = null;
    const results = [];
    stream.on('error', err => reject(Object.assign(err, { inTransaction })));
    stream.on('end', _ => {
      const lastResult = results[results.length - 1];
      const rows = lastResult && lastResult.rows;
      let scalar = rows && rows[0];
      if (Array.isArray(scalar)) { // DataRow or CopyData
        scalar = scalar[0];
      }
      return resolve({
        inTransaction,
        results,
        rows,
        scalar,
        command: lastResult && lastResult.command,
        empty: Boolean(lastResult && lastResult.empty),
        suspended: Boolean(lastResult && lastResult.suspended),
      });
    });
    stream.on('data', msg => {
      switch (msg.tag) {
        case 'DataRow':
        case 'CopyData':
          rows.push(msg.data);
          break;
        case 'CommandComplete':
          results.push({ rows, command: msg.command });
          rows = [];
          break;
        case 'PortalSuspended':
          results.push({ rows, suspended: true });
          rows = [];
          break;
        case 'EmptyQueryResponse':
          results.push({ rows, empty: true });
          rows = [];
          break;
        case 'ReadyForQuery':
          inTransaction = msg.transactionStatus != 'I'.charCodeAt();
      }
    });
  });
}
