const net = require('net');
const EventEmmiter = require('events');
const { Readable, PassThrough, Transform, pipeline, finished } = require('stream');
const { createHash, pbkdf2, createHmac, randomBytes } = require('crypto');
const { debuglog } = require('util');
const BackendDecoder = require('./backend.js');
const { FrontendEncoder, ...fe } = require('./frontend.js');
const { logicalReplication } = require('./replication.js');
const { datatypesById, datatypesByName } = require('./datatypes.js');

const logBeMsg = debuglog('pgwire-be');

module.exports =
class Connection extends EventEmmiter {
  constructor({ idleTimeout, ...options }) {
    super();
    this.parameters = {};
    this._idleTimeoutMillis = Math.max(idleTimeout, 0);
    this._idleTimer = null;
    this._options = options;
    this._responses = [];
    this._rowDecoder = null;
    this._pgerror = null;
    this._closeError = null;
    this._socket = null;
    this._tx = new FrontendEncoder();
    this._startuptx = new FrontendEncoder();
    this._tx.write(new fe.SubProtocol(this._startuptx));
    this._rx = new BackendDecoder();
    this._rx.on('data', this._recvMessage.bind(this));
    this._isCopyOutMode = false;
  }
  query(arg, ...args) {
    if (typeof arg == 'string') {
      return this._querySimple(arg, ...args);
    }
    return this._queryExtended([arg].concat(args));
  }
  _querySimple(script, { stdin } = {}) {
    const outmsg = [new fe.Query(script)];
    if (stdin) {
      outmsg.push(...[].concat(stdin).map(
        it => new fe.SubProtocol(copyin(it)),
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
  _queryExtended(extendedOps) {
    return this._request(
      extendedOps
      .map(script2msg)
      .reduce((flat, it) => (flat.push(...it), flat), [])
      .concat(new fe.Sync()),
    );

    function script2msg(it) {
      switch (it.op) {
        case undefined:
        case null:
          return [
            ...script2msg({
              op: 'parse',
              statement: it.statement,
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
            outFormats0t1b: [1],
            ...it,
            params: it.params && it.params.map(({ type, value }) => (
              type ? findDatatype(type).encode(value) : value
            )),
          })];
        case 'execute':
          return [
            // TODO write test to explain why
            // we need unconditional DescribePortal
            // before Execute
            new fe.DescribePortal(it.portal),
            new fe.Execute(it),
            it.stdin
              ? new fe.SubProtocol(copyin(it.stdin))
              // CopyFail message ignored by postgres
              // if there is no COPY FROM statement
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
  destroy(err) {
    this._rx.destroy(err);
  }
  get queueSize() {
    return this._responses.length;
  }
  async logicalReplication(options) {
    const replstream = await logicalReplication(this, options);
    // TODO: respect ?keepalives=0
    this._socket.setKeepAlive(true);
    return replstream;
  }

  _connectIfNotConnected() {
    if (this._connectionInited) {
      return false;
    }
    this._connectionInited = true;
    const {
      unixSocket,
      hostname,
      port,
      password: _password,
      ...startupParameters
    } = this._options;
    const socket = this._socket = net.connect(unixSocket || { host: hostname, port });

    // cannot use stream.pipeline https://github.com/exe-dealer/node-iss-33050
    this._tx
    .on('error', err => socket.destroy(err))
    .pipe(socket)
    .on('error', err => this._rx.destroy(err))
    .pipe(this._rx)
    .on('error', _ => socket.destroy());

    finished(this._rx, this._onFinished.bind(this));

    this._startuptx.write(new fe.StartupMessage(startupParameters));
    return true;
  }
  _recvMessage(msg) {
    logBeMsg('-> %j', msg);
    switch (msg.tag) {
      case 'DataRow': return this._recvDataRow(msg);
      case 'CopyData': return this._recvCopyData(msg);
      case 'CopyDone': return this._recvCopyDone(msg);
      case 'CopyOutResponse': return this._recvCopyOutResponse(msg);
      case 'CopyBothResponse': return this._recvCopyBothResponse(msg);
      case 'CommandComplete': return this._recvCommandComplete(msg);
      case 'EmptyQueryResponse': return this._recvEmptyQueryResponse(msg);
      case 'PortalSuspended': return this._recvPortalSuspended(msg);
      case 'NoData': return this._recvNoData(msg);
      case 'RowDescription': return this._recvRowDescription(msg);
      case 'ReadyForQuery': return this._recvReadyForQuery(msg);
      case 'ErrorResponse': return this._recvErrorResponse(msg);
      case 'NotificationResponse': return this._recvNotificationResponse(msg);
      case 'NoticeResponse': return this._recvNoticeResponse(msg);
      case 'ParameterStatus': return this._recvParameterStatus(msg);
      case 'BackendKeyData': return this._recvBackendKeyData(msg);
      case 'AuthenticationCleartextPassword': return this._recvAuthenticationCleartextPassword(msg);
      case 'AuthenticationMD5Password': return this._recvAuthenticationMD5Password(msg);
      case 'AuthenticationSASL': return this._recvAuthenticationSASL(msg);
      case 'AuthenticationSASLContinue': return this._recvAuthenticationSASLContinue(msg);
      case 'AuthenticationSASLFinal': return this._recvAuthenticationSASLFinal(msg);
    }
  }
  _passResponse(chunk) {
    const response = this._responses[0];
    return response.push(chunk) || response.destroyed;
  }
  _passResponseBackpreasured(chunk) {
    if (!this._passResponse(chunk)) {
      this._rx.pause();
    }
  }
  _passBoundary(boundary) {
    return this._passResponse(Object.assign(
      Buffer.alloc(0),
      { boundary },
    ));
  }
  _recvReadyForQuery(msg) {
    if (this._startuptx) {
      this._startuptx.end();
      this._startuptx = null;
      this.emit('ready');
    } else {
      this._passBoundary(msg);
      const response = this._responses.shift();
      response.emit('readyForQuery', msg);
      if (this._pgerror) {
        response.destroy(this._pgerror);
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
    this._socket = null;
    // TODO what error should we report
    // if we have both ioerr and this._pgerror ?
    const err = this._closeError = ioerr || this._pgerror;
    this.emit('close', err);
    while (this._responses.length) {
      this._responses.shift().destroy(err || Error('Connection terminated'));
    }
  }
  _request(messages) {
    const response = new Response(this);
    if (!this._tx) {
      return response.destroy(Object.assign(Error(), {
        message: 'Query after connection end',
        closeError: this._closeError,
      }));
    }
    clearTimeout(this._idleTimer);
    this._connectIfNotConnected();
    for (const m of messages) {
      this._tx.write(m);
    }
    this._responses.push(response);
    return response;
  }

  _recvErrorResponse({ tag: _, code, ...props }) {
    this._isCopyOutMode = false;
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
    this._passBoundary(msg);
  }
  _recvNoData(msg) {
    this._passBoundary(msg);
  }
  _recvDataRow({ data }) {
    for (let i = 0; i < data.length; i++) {
      const val = data[i];
      if (val != null) {
        data[i] = this._rowDecoder[i](val);
      }
    }
    this._passResponseBackpreasured(data);
  }
  _recvCopyData({ data }) {
    // postgres can send CopyData when not in copy out mode
    // steps to reproduce:
    //
    // StartupMessage({ database: 'postgres', user: 'postgres', replication: 'database' })
    //
    // Query('CREATE_REPLICATION_SLOT a LOGICAL test_decoding')
    // wait for ReadyForQuery
    //
    // Query('START_REPLICATION SLOT a LOGICAL 0/0');
    //
    // CopyDone() // before wal_sender_timeout expires
    // copydata-keepalive message can be received here after CopyDone
    // but before ReadyForQuery
    //
    // Query('CREATE_REPLICATION_SLOT b LOGICAL test_decoding')
    // sometimes copydata-keepalive message can be received here
    // before RowDescription message
    if (this._isCopyOutMode) {
      this._passResponseBackpreasured(data);
    }
  }
  _recvCopyDone(msg) {
    this._isCopyOutMode = false;
    this._passBoundary(msg);
  }
  _recvCopyOutResponse(msg) {
    this._isCopyOutMode = true;
    this._passBoundary(msg);
  }
  _recvCopyBothResponse(msg) {
    this._isCopyOutMode = true;
    this._passBoundary(msg);
  }
  _recvCommandComplete(msg) {
    this._passBoundary(msg);
  }
  _recvEmptyQueryResponse(msg) {
    this._passBoundary(msg);
  }
  _recvPortalSuspended(msg) {
    this._passBoundary(msg);
  }
  _recvNotificationResponse(msg) {
    this.emit('notification', msg);
  }
  _recvParameterStatus(msg) {
    this.parameters[msg.parameter] = msg.value;
    this.emit('parameter', msg.parameter, msg.value);
  }
  _recvBackendKeyData({ pid, secretkey }) {
    this.pid = pid;
    this.secretkey = secretkey;
  }
  _recvNoticeResponse(msg) {
    if (this._responses[0]) {
      if (!this._passBoundary(msg)) {
        this._rx.pause();
      }
      return;
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
  _recvAuthenticationSASL(_msg) {
    randomBytes(24, (err, clientNonce) => {
      if (err) {
        return this._rx.destroy(err);
      }
      const clientNonceB64 = clientNonce.toString('base64');
      this._saslClientFirstMessageBare = 'n=,r=' + clientNonceB64;
      this._startuptx.write(new fe.SASLInitialResponse({
        mechanism: 'SCRAM-SHA-256',
        data: 'n,,' + this._saslClientFirstMessageBare,
      }));
    });
  }
  _recvAuthenticationSASLContinue({ data }) {
    const firstServerMessage = data.toString();
    const { i: iter, s: saltB64, r: nonceB64 } = (
      firstServerMessage
      .split(',')
      .map(it => /^([^=]*)=(.*)$/.exec(it))
      .reduce((acc, [, key, val]) => ({ ...acc, [key]: val }), {})
    );
    const finalMessageWithoutProof = 'c=biws,r=' + nonceB64;
    const salt = Buffer.from(saltB64, 'base64');
    const password = this._options.password.normalize();
    pbkdf2(password, salt, +iter, 32, 'sha256', (err, saltedPassword) => {
      if (err) {
        return this._rx.destroy(err);
      }
      const hmac = (key, inp) => createHmac('sha256', key).update(inp).digest();
      const clientKey = hmac(saltedPassword, 'Client Key');
      const storedKey = createHash('sha256').update(clientKey).digest();
      const authMessage = (
        this._saslClientFirstMessageBare + ',' +
        firstServerMessage + ',' +
        finalMessageWithoutProof
      );
      const clientSignature = hmac(storedKey, authMessage);
      const clientProof = Buffer.from(clientKey.map((b, i) => b ^ clientSignature[i]));
      const serverKey = hmac(saltedPassword, 'Server Key');
      this._saslServerSignatureB64 = hmac(serverKey, authMessage).toString('base64');
      this._startuptx.write(new fe.SASLResponse(
        finalMessageWithoutProof + ',p=' + clientProof.toString('base64'),
      ));
    });
  }
  _recvAuthenticationSASLFinal({ data }) {
    const receivedServerSignatureB64 = /(?<=^v=)[^,]*/.exec(data);
    if (this._saslServerSignatureB64 != receivedServerSignatureB64) {
      this._rx.destroy(Error('Invalid server SCRAM signature'));
    }
  }
  _onIdleTimeout() {
    // this.emit('idletimeout');
    this.end();
  }
};

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

class Response extends Readable {
  constructor(conn) {
    super({ objectMode: true });
    this._conn = conn;
    this._promise = null;
    // save stacktrace to understand what query causes error
    this._fetchError = Error();
  }
  _read() {
    this._resumeConIfCurrentResponse();
  }
  _destroy(...args) {
    this._resumeConIfCurrentResponse();
    return super._destroy(...args);
  }
  _resumeConIfCurrentResponse() {
    if (this == this._conn._responses[0]) {
      this._conn._rx.resume();
    }
  }
  _fetch() {
    if (!this._promise) {
      this._promise = fetchResponse(this).catch(err => Promise.reject(
        Object.assign(this._fetchError, err),
      ));
    }
    return this._promise;
  }
  then(...args) {
    return this._fetch().then(...args);
  }
  catch(...args) {
    return this._fetch().catch(...args);
  }
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
    stream.on('data', chunk => {
      const { boundary } = chunk;
      if (!boundary) {
        return rows.push(chunk);
      }
      switch (boundary.tag) {
        case 'NoticeResponse':
          notices.push(boundary);
          break;
        case 'CommandComplete':
          results.push({ rows, notices, command: boundary.command });
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
          inTransaction = boundary.transactionStatus != 'I'.charCodeAt();
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
