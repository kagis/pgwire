const net = require('net');
const EventEmmiter = require('events');
const { Readable, pipeline } = require('stream');
const { BackendDecoder } = require('./backend.js');
const datatypes = require('./datatypes.js');
const fe = require('./frontend.js');
const { logicalReplication } = require('./replication.js');
const { pgliteral, pgident, pgarray } = require('./escape.js');

const config = url2conf(process.env.POSTGRES);

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
      null,
    ),
  }
}

function configNormalize(optionsOrUrl) {
  if (typeof optionsOrSql == 'string') {
    return url2conf(optionsOrUrl);
  }
  return optionsOrUrl;
}

function oneshot(options) {
  return new Oneshot(options);
}

async function connectRetry(options) {
  for (;;) {
    try {
      return await connect(options);
    } catch (err) {
      if (!(
        err.code == 'ENOTFOUND' ||
        err.code == 'ECONNREFUSED' ||
        err.code == 'ECONNRESET' ||
        err.code == 'PGERR_57P03' // cannot_connect_now
      )) {
        throw err;
      }
      // console.error(
      //   `postgres is not available (${err.code}),`,
      //   `will try again after ${retry_interval_s}s`,
      // );
      await new Promise(resolve => setTimeout(resolve, 1e3));
    }
  }
}

async function connect(options) {
  const conn = new Connection();
  await conn.connect(options);
  return conn;
}

function pool(options) {
  return new Pool(options);
}

class ClientBase extends EventEmmiter {
  query(optionsOrSql) {
    return fetchResponse(this.queryStream(optionsOrSql));
  }
  queryStream(optionsOrSql) {
    let { sql, stdin } = optionsOrSql;
    if (typeof optionsOrSql == 'string') {
      sql = optionsOrSql;
    }
    const outmsg = [new fe.Query(sql)];
    if (stdin) {
      for (const it of [].concat(stdin)) {
        outmsg.push(new fe._CopyUpstream(it));
      }
    }
    let onReadyForQuery;
    if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(sql)) {
      // should wait for ReadyForQuery
      outmsg.push(new fe._Block(new Promise(resolve =>  {
        onReadyForQuery = resolve;
      })));
    } else {
      outmsg.push(new fe.CopyFail('Missing copy upstream'));
    }
    const response = this._request(outmsg);
    if (onReadyForQuery) {
      response.on('readyForQuery', onReadyForQuery);
    }
    return response;
  }
  queryExtended() {
    return new ExtendedQuery(this);
  }
  logicalReplication(options) {
    return logicalReplication(this, options);
  }
  session(_cb, _options) {
    throw Error('not implemented');
  }
  _request(_messages) {
    throw Error('not implemented');
  }
}

class Oneshot extends ClientBase {
  constructor(options = config) {
    super();
    this._connOptions = configNormalize(options);
  }
  _request(messages) {
    const conn = new Connection();
    const connResult = conn.connect(this._connOptions);
    let response;
    try {
      response = conn._request(messages);
    } finally {
      conn.end();
    }
    connResult.catch(err => {
      response.emit('error', err);
    });
    return response;
  }
  async session(cb, options) {
    const conn = await connect({ ...this._connOptions, ...options });
    try {
      return await cb(conn);
    } finally {
      conn.end();
    }
  }
}

class Connection extends ClientBase {
  constructor() {
    super();
    this.parameters = {};
    this._pendingResponses = [];
    this._response = null;
    this._socket = null;
    this._rx = null;
    this._tx = null;
  }
  connect(options = config) {
    options = configNormalize(options);
    const { hostname, port, password, ...parameters } = options;
    this._socket = net.connect({ host: hostname, port });
    this._rx = new BackendDecoder();
    this._tx = new fe.FrontendEncoder();
    this._rx.on('data', this._recvMessage.bind(this));
    pipeline(this._tx, this._socket, this._rx, this._onFinished.bind(this));

    return fetchResponse(this._request([
      new fe.StartupMessage(parameters),
    ]));
  }
  end() {
    this._tx.write(new fe.Terminate());
    this._tx.end();
  }
  session(cb) {
    return cb(this);
  }

  _recvMessage(msg) {
    // console.error('->', JSON.stringify(msg));
    switch (msg.tag) {
      case 'DataRow':
        msg = this._decodeRow(msg);
        break;
      case 'RowDescription':
        this._recvRowDescription(msg);
        break;
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
    this._response.emit('readyForQuery', msg);
    this._finishResponse();
  }
  _finishResponse() {
    const response = this._response;
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
  _recvError({ tag: _, code, ...props }) {
    const err = Object.assign(new Error(), props, {
      name: 'PGError',
      code: 'PGERR_' + code,
    });
    if (!this._response) {
      return this.emit('error', err);
    }
    // this._response.emit('errorResponse', err);
    this._response._pgerror = Object.assign(err, {
      prevError: this._response._pgerror,
    });
  }
  _recvRowDescription(msg) {
    this._rowDecoder = msg.fields.map(({ typeid, binary }) => {
      const { decodeBin, decodeText } = datatypes[typeid] || datatypes.noop;
      if (binary) {
        return decodeBin;
      }
      return buf => decodeText(String(buf));
    });
  }
  _decodeRow(msg) {
    for (let i = 0; i < msg.data.length; i++) {
      const val = msg.data[i];
      if (val != null) {
        msg.data[i] = this._rowDecoder[i](val);
      }
    }
    return msg;
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
  _onFinished(err) {
    if (this._response) {
      this._response.emit('error', err || Error('Connection terminated'));
      while (this._pendingResponses.length) {
        this._pendingResponses.shift().emit('error', err || Error('Connection terminated'));
      }
    } else if (err) {
      this.emit('error', err);
    }
  }
  _request(messages) {
    for (const m of messages) {
      this._tx.write(m);
    }
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

class Pool extends ClientBase {
  constructor(options) {
    this._options = options;
    this._connections = [];
    for (let i = 0; i < 4; i++) {
      const conn = new Connection(options);
      conn.connect().catch(err => this.emit('error', err));
      this._connections.push(conn);
    }
  }
  _request(messages) {
    const conn = this._connections.shift();
    this._connections.push(conn);
    return conn._request(messages);
  }
}

class ExtendedQuery {
  constructor(conn) {
    this._conn = conn;
    this._messages = [];
  }
  parse(options) {
    if (options.paramTypes) {
      options.paramTypes = options.paramTypes.map(ptype => {
        if (Number.isInteger(Number(ptype))) {
          return Number(ptype);
        }
        const { id = 0 } = datatypes[ptype] || {};
        return id;
      });
    }
    this._messages.push(new fe.Parse(options));
    return this;
  }
  describeStatement(statementName) {
    this._messages.push(new fe.DescribeStatement(statementName));
    return this;
  }
  bind({ name, portal, params } = {}) {
    this._messages.push(new fe.Bind({
      name,
      portal,
      params,
      outFormats0t1b: [1],
    }));
    return this;
  }
  describePortal(portalName) {
    this._messages.push(new fe.DescribePortal(portalName));
    return this;
  }
  execute(options) {
    this._messages.push(new fe.DescribePortal(options && options.portal));
    this._messages.push(new fe.Execute(options));
    if (options && options.stdin) {
      this._messages.push(new fe._CopyUpstream(options.stdin));
    } else {
      this._messages.push(new fe.CopyFail('Missing copy upstream'));
    }
    return this;
  }
  closeStatement(statementName) {
    this._messages.push(new fe.CloseStatement(statementName));
    return this;
  }
  closePortal(portalName) {
    this._messages.push(new fe.ClosePortal(portalName));
    return this;
  }
  stream() {
    return this._conn._request(this._messages.concat(new fe.Sync()));
  }
  fetch(options) {
    return fetchResponse(this.stream(options));
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
        case 'RowDescription':
          rowDescription = msg;
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
          results.push({
            rows,
            empty: true,
          });
          rows = [];
          break;
        // case 'ParseComplete':
        // case 'BindComplete':
        //   results.push({

        //   });
        //   rows = [];
        //   break;

        case 'ReadyForQuery':
          inTransaction = msg.transactionStatus != 'I'.charCodeAt();
      }
    });
  });
}

module.exports = Object.assign(oneshot(), {
  connect,
  connectRetry,
  oneshot,
  pool,
  config,
  pgliteral,
  pgident,
  pgarray,
});
