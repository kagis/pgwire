const net = require('net');
const EventEmmiter = require('events');
const { Readable, PassThrough, Transform, pipeline } = require('stream');
const { createHash } = require('crypto');
const { debuglog } = require('util');
const BackendDecoder = require('./backend.js');
const { FrontendEncoder, ...fe } = require('./frontend.js');
const { logicalReplication } = require('./replication.js');
const { datatypesById, datatypesByName } = require('./datatypes.js');

const logBeMsg = debuglog('pgwire-be');

class ClientBase extends EventEmmiter {
  async query(options) {
    return fetchResponse(this.stream(options));
  }
  stream(_options) {
    throw Error('not implemented');
  }
  logicalReplication(_options) {
    throw Error('not implemented');
  }
}

class Connection extends ClientBase {
  constructor({ idleTimeout, ...options }) {
    super();
    this.parameters = {};
    this._idleTimeoutMillis = Number(idleTimeout) || 0;
    this._idleTimer = null;
    this._options = options;
    this._responses = [];
    this._rowDecoder = null;
    this._pgerror = null;
    this._tx = new FrontendEncoder();
    this._startuptx = new FrontendEncoder();
    this._tx.write(new fe.SubProtocol(this._startuptx));
    this._rx = new BackendDecoder();
    this._rx.on('data', this._recvMessage.bind(this));
  }
  stream(optionsOrScript) {
    if (typeof optionsOrScript == 'string') {
      return this._streamSimple({ script: optionsOrScript });
    }
    if (optionsOrScript.extended) {
      return this._streamExtended(optionsOrScript);
    }
    return this._streamSimple(optionsOrScript);
  }
  _streamSimple({ script, stdin }) {
    const outmsg = [new fe.Query(script)];
    if (stdin) {
      outmsg.push(...[].concat(stdin).map(
        it => new fe.SubProtocol(copyin(it))
      ));
    }
    let onReadyForQuery;
    if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(script)) {
      // should wait for ReadyForQuery
      const blocktx = new PassThrough();
      outmsg.push(new fe.SubProtocol(blocktx));
      onReadyForQuery = _ => blocktx.end();
    } else {
      outmsg.push(new fe.CopyFail('Missing copy upstream'));
    }
    const response = this._request(outmsg);
    if (onReadyForQuery) {
      response.on('readyForQuery', onReadyForQuery);
    }
    return response;
  }
  _streamExtended({ script }) {
    return this._request(
      script
      .map(script2msg)
      .reduce((flat, it) => (flat.push(...it), flat), [])
      .concat(new fe.Sync())
    );

    function script2msg(it) {
      switch (it.op) {
        case undefined:
        case null:
          return [
            ...script2msg({
              op: 'parse',
              sql: it.sql,
              paramTypes: it.params && it.params.map(({ type }) => type),
            }),
            ...script2msg({
              op: 'bind',
              params: it.params,
            }),
            ...script2msg({
              op: 'execute',
              limit: it.limit,
              stdin: it.stdin,
            }),
          ];
        case 'parse':
          return [new fe.Parse({
            ...it,
            paramTypes: it.paramTypes && it.paramTypes.map(normalizeDatatypeId),
          })];
        case 'bind':
          return [new fe.Bind({
            portal: it.portal,
            outFormats0t1b: [1],
            params: it.params && it.params.map(({ type, value }) => (
              type ? findDatatype(type).encode(value) : value
            )),
          })];
        case 'execute':
          return [
            new fe.DescribePortal(it.portal),
            new fe.Execute(it),
            it.stdin
              ? new fe.SubProtocol(copyin(it.stdin))
              : new fe.CopyFail('Missing copy upstream'),
          ];
        case 'describe-statement':
          return [new fe.DescribeStatement(it.statementName)];
        case 'close-statement':
          return [new fe.CloseStatement(it.statementName)];
        case 'describe-portal':
          return [new fe.DescribePortal(it.portal)];
        case 'close-portal':
          return [new fe.ClosePortal(it.portal)];
        default:
          throw Error(`Unknown op "${it.op}"`);
      }
    }
  }
  end() {
    if (!this._tx) {
      return;
    }
    if (this._responses.length) {
      // pgbouncer closes connection before return query result
      // if we send Terminate message asynchronously,
      // so we need to wait for all responses before terminate
      const [lastResponse] = this._responses.slice(-1);
      const blocktx = new PassThrough();
      lastResponse.on('readyForQuery', _ => blocktx.end());
      this._tx.write(new fe.SubProtocol(blocktx));
    }
    this._tx.write(new fe.Terminate());
    this._tx.end();
    this._tx = null;
  }
  get queueSize() {
    return this._responses.length;
  }
  logicalReplication(options) {
    return logicalReplication(this, options);
  }

  _connectIfNotConnected() {
    if (this._connectionInited) {
      return false;
    }
    this._connectionInited = true;
    const {
      hostname,
      port,
      password: _password,
      ...startupParameters
    } = this._options;
    const socket = net.connect({ host: hostname, port });
    pipeline(this._tx, socket, this._rx, this._onFinished.bind(this));
    this._startuptx.write(new fe.StartupMessage(startupParameters));
    return true;
  }
  _recvMessage(msg) {
    logBeMsg('-> %j', msg);
    switch (msg.tag) {
      case 'DataRow': return this._recvDataRow(msg);
      case 'RowDescription': return this._recvRowDescription(msg);
      case 'ReadyForQuery': return this._recvReadyForQuery(msg);
      case 'ErrorResponse': return this._recvError(msg);
      case 'NotificationResponse': return this._recvNotification(msg);
      case 'NoticeResponse': return this._recvNotice(msg);
      case 'ParameterStatus': return this._recvParameterStatus(msg);
      case 'BackendKeyData': return this._recvBackendKeyData(msg);
      case 'AuthenticationCleartextPassword': return this._recvAuthenticationCleartextPassword(msg);
      case 'AuthenticationMD5Password': return this._recvAuthenticationMD5Password(msg);
      case 'AuthenticationOk': return;
      // case 'AuthenticationSASL':
      //   return tx.write(new fe.SASLInitialResponse({
      //     mechanism: 'SCRAM-SHA-256',
      //     data: 'client-first-message'
      //   }));
      default: return this._passResponse(msg);
    }
  }
  _passResponse(msg) {
    if (!this._responses[0].push(msg)) {
      this._rx.pause();
    }
  }
  _recvReadyForQuery(msg) {
    if (this._startuptx) {
      this._startuptx.end();
      this._startuptx = null;
      this.emit('ready');
    } else {
      const response = this._responses.shift();
      response.push(msg);
      response.emit('readyForQuery', msg);
      if (this._pgerror) {
        setImmediate(err => response.emit('error', err), this._pgerror);
        this._pgerror = null;
      } else {
        response.push(null);
      }
    }
    if (this._idleTimeoutMillis > 0 && !this._responses.length) {
      this._idleTimer = setTimeout(
        this._onIdleTimeout.bind(this),
        this._idleTimeoutMillis,
      );
    }
  }
  _onFinished(ioerr) {
    clearTimeout(this._idleTimer);
    this._tx = null;
    // TODO what error should we report
    // if we have both ioerr and this._pgerror ?
    const err = this._pgerror || ioerr;
    setImmediate(_ => this.emit('close', err));
    if (err && !this._responses.length) {
      return this.emit('error', err);
    }
    while (this._responses.length) {
      const response = this._responses.shift();
      setImmediate(_ => response.emit('error', err || Error('Connection terminated')));
    }
  }
  _request(messages) {
    if (!this._tx) {
      throw Error('Query after connection end');
    }
    clearTimeout(this._idleTimer);
    this._connectIfNotConnected();
    for (const m of messages) {
      this._tx.write(m);
    }
    const response = new Readable({
      objectMode: true,
      read: _ => {
        if (response == this._responses[0]) {
          this._rx.resume();
        }
      },
    });
    this._responses.push(response);
    return response;
  }
  _recvError(msg) {
    const { tag: _, code, ...props } = msg;
    this._pgerror = Object.assign(Error(), props, {
      name: 'PGError',
      code: 'PGERR_' + code,
      prevError: this._pgerror,
    });
  }
  _recvRowDescription(msg) {
    this._rowDecoder = msg.fields.map(({ typeid, binary }) => {
      const {
        decodeBin = noop => noop,
        decodeText = noop => noop,
      } = datatypesById[typeid] || {};
      if (binary) {
        return decodeBin;
      }
      return buf => decodeText(String(buf));
    });
    this._passResponse(msg);
  }
  _recvDataRow(msg) {
    for (let i = 0; i < msg.data.length; i++) {
      const val = msg.data[i];
      if (val != null) {
        msg.data[i] = this._rowDecoder[i](val);
      }
    }
    this._passResponse(msg);
  }
  _recvNotification(msg) {
    this.emit('notify', msg);
    this.emit('notify:' + msg.channel, msg);
  }
  _recvParameterStatus(msg) {
    this.parameters[msg.parameter] = msg.value;
    this.emit('parameter', msg.parameter, msg.value);
  }
  _recvBackendKeyData({ pid, secretkey }) {
    this.pid = pid;
    this.secretkey = secretkey;
  }
  _recvNotice(msg) {
    if (this._responses[0]) {
      return this._passResponse(msg);
    }
    if (this.emit('notice', msg)) {
      return;
    }
    console.error(msg);
  }
  _recvAuthenticationCleartextPassword() {
    // if (!this._options.password) {
    //   return this._startuptx.destroy(Error('Password was not specified'));
    // }
    this._startuptx.write(new fe.PasswordMessage(this._options.password));
  }
  _recvAuthenticationMD5Password({ salt }) {
    const pwdhash = (
      createHash('md5')
      .update(this._options.password, 'utf-8')
      .update(this._options.user, 'utf-8')
      .digest('hex')
    );
    this._startuptx.write(new fe.PasswordMessage('md5' + (
      createHash('md5')
      .update(pwdhash, 'utf-8')
      .update(salt)
      .digest('hex')
    )));
  }
  _onIdleTimeout() {
    // this.emit('idletimeout');
    this.end();
  }
}

function copyin(upstream) {
  const feEncoder = new FrontendEncoder();
  const msgstream = pipeline(upstream, new Transform({
    readableObjectMode: true,
    transform: (chunk, _enc, done) => done(null, new fe.CopyData(chunk)),
  }), err => {
    if (err) {
      feEncoder.end(new fe.CopyFail(String(err)));
    } else {
      feEncoder.end(new fe.CopyDone());
    }
  });
  feEncoder.on('error', err => msgstream.destroy(err));
  return msgstream.pipe(feEncoder, { end: false });
}

function fetchResponse(stream) {
  return new Promise((resolve, reject) => {
    let rows = [];
    let notices = [];
    let inTransaction = null;
    const results = [];
    stream.on('error', err => reject(Object.assign(err, { inTransaction })));
    stream.on('end', _ => {
      const lastResult = results[results.length - 1];
      const lastRows = lastResult && lastResult.rows;
      let scalar = lastRows && lastRows[0];
      if (Array.isArray(scalar)) { // DataRow or CopyData
        scalar = scalar[0];
      }
      return resolve({
        inTransaction,
        results,
        rows: lastRows,
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
        case 'NoticeResponse':
          notices.push(msg);
          break;
        case 'CommandComplete':
          results.push({ rows, notices, command: msg.command });
          rows = [];
          notices = [];
          break;
        case 'PortalSuspended':
          results.push({ rows, notices, suspended: true });
          rows = [];
          notices = [];
          break;
        case 'EmptyQueryResponse':
          results.push({ rows, notices, empty: true });
          rows = [];
          notices = [];
          break;
        case 'ReadyForQuery':
          inTransaction = msg.transactionStatus != 'I'.charCodeAt();
          break;
      }
    });
  });
}

function normalizeDatatypeId(typeIdOrName) {
  if (typeof typeIdOrName == 'number') {
    return typeIdOrName;
  }
  const result = datatypesByName[typeIdOrName];
  if (result) {
    return result.id;
  }
  throw Error(`Unknown builtin datatype name ${JSON.stringify(typeIdOrName)}`);
}

function findDatatype(typeIdOrName) {
  if (typeof typeIdOrName == 'number') {
    const result = datatypesById[typeIdOrName];
    if (result) {
      return result;
    }
    throw Error(`Unknown builtin datatype id ${typeIdOrName}`);
  } else {
    const result = datatypesByName[typeIdOrName];
    if (result) {
      return result;
    }
    throw Error(`Unknown builtin datatype name ${JSON.stringify(typeIdOrName)}`);
  }
}

module.exports = {
  ClientBase,
  Connection,
};
