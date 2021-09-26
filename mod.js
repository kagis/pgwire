/**
 * @typedef {{
 *  hostname: string,
 *  port: number,
 *  user: string,
 *  password: string,
 *  database: string,
 *  application_name: string,
 * }} PGConnectOptions
 * */

export const _networking = {
  async connect(options) {
    return Deno.connect(options);
  },
  isReconnectableError(err) {
    return (
      err instanceof Deno.errors.ConnectionRefused ||
      err instanceof Deno.errors.ConnectionReset
    );
  },
  close(socket) {
    try {
      socket.close();
    } catch (err) {
      if (!(err instanceof Deno.errors.BadResource)) {
        throw err;
      }
    }
  },
};

/** @param  {...(string|URL|PGConnectOptions)} options */
export async function pgconnect(...options) {
  let { '.connectRetry': connectRetry, ...connOptions } = computeConnectionOptions(options);
  // const connectRetry = [1, '1', 'on', 'true', true].includes(connectRetryRaw);
  const startTime = Date.now();
  for (;;) {
    try {
      const conn = new Connection(connOptions);
      await conn.whenReady;
      return conn;
    } catch (err) {
      const elapsedTime = Date.now() - startTime;
      if (elapsedTime < connectRetry && (
        _networking.isReconnectableError(err) ||
        pgerrcode(err) == '57P03' // cannot_connect_now
      )) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      throw err;
    }
  }
}

// /** @param  {...(string|URL|PGConnectOptions)} options */
// export function pgconnect(...options) {
//   const connection = new Connection(computeConnectionOptions(options));
//   const p = Promise.resolve(connection.whenReady);
//   p.connection = connection; // https://github.com/kagis/pgwire/issues/15
//   p.catch(Boolean);
//   return p;
// }

/** @param  {...(string|URL|PGConnectOptions)} options */
export function pgpool(...options) {
  return new Pool(computeConnectionOptions(options));
}

function computeConnectionOptions([uriOrObj, ...rest]) {
  if (!uriOrObj) {
    return { hostname: '127.0.0.1', port: 5432 };
  }
  if (typeof uriOrObj == 'string' || uriOrObj instanceof URL) {
    const uri = new URL(uriOrObj);
    // if (!(uri.protocol == 'postgresql' || uri.protocol == 'postgres')) {
    //   throw Error(`invalid postgres protocol ${JSON.stringify(uri.protocol)}`);
    // }
    uriOrObj = Array.from(uri.searchParams).reduce(
      (obj, [k, v]) => (obj[k] = v, obj),
      withoutUndefinedProps({
        hostname: uri.hostname || undefined,
        port: Number(uri.port) || undefined,
        password: uri.password || undefined,
        'user': uri.username || undefined,
        'database': uri.pathname.replace(/^\//, '') || undefined,
      }),
    );
  }
  return Object.assign(computeConnectionOptions(rest), uriOrObj);

  function withoutUndefinedProps(obj) {
    return JSON.parse(JSON.stringify(obj));
  }
}

export function pgerrcode(err) {
  return err?.[kErrorPgcode];
}

const kErrorPgcode = Symbol('kErrorPgcode');

class PgError extends Error {
  constructor({ message, ...props }) {
    super(message);
    this.name = 'PgError';
    this[kErrorPgcode] = props.code;
    Object.assign(this, props);
  }
}

class Pool {
  constructor({ '.poolSize': poolSize, '.poolIdleTimeout': poolIdleTimeout, ...options }) {
    this._connections = /** @type {Set<Connection>} */ new Set();
    this._ended = false;
    this._options = options;
    this._poolSize = Math.max(poolSize, 0);
    // postgres v14 has `idle_session_timeout` option.
    // But if .query will be called when socket is closed by server
    // (but Connection is not yet deleted from pool)
    // then query will be rejected with error.
    // We should be ready to repeat query in this case
    // and this is more complex than client side timeout
    this._poolIdleTimeout = Math.max(poolIdleTimeout, 0);
  }
  query(...args) {
    return new Response(this._query(args));
  }
  async * _query(args) {
    const conn = this._getConnection();
    try {
      yield * conn.query(...args);
    } finally {
      // TODO AggregateError
      await this._recycleConnection(conn);
    }
  }
  async end() {
    this._ended = true;
    await Promise.all(Array.from(this._connections, conn => conn.end()));
  }
  destroy(destroyReason) {
    this._ended = true;
    this._connections.forEach(it => void it.destroy(destroyReason))
    return destroyReason; // should keep and throw destroyReason when querying ?
  }
  _getConnection() {
    if (this._ended) {
      throw Error('pool is not usable anymore');
    }
    if (!this._poolSize) {
      return new Connection(this._options);
    }
    let leastBusyConn;
    for (const conn of this._connections) {
      if (!(leastBusyConn?.pending < conn.pending)) {
        leastBusyConn = conn;
      }
    }
    const hasSpaceForNewConnection = this._connections.size < this._poolSize;
    if (leastBusyConn?.pending == 0 || !hasSpaceForNewConnection) {
      clearTimeout(leastBusyConn._poolTimeoutId);
      return leastBusyConn;
    }
    const newConn = new Connection(this._options);
    newConn._poolTimeoutId = null; // TODO check whether it disables v8 hidden class optimization
    this._connections.add(newConn);
    newConn.whenEnded.then(this._onConnectionEnded.bind(this, newConn));
    return newConn;
  }
  async _recycleConnection(conn) {
    if (!this._poolSize) {
      // TODO not wait
      // but pgbouncer does not allow async Terminate anyway.
      // May be other poolers do
      return conn.end();
    }

    if (conn.inTransaction) {
      // - discard all
      // people say that it is slow as creating new connection.
      // it will cause error when connection is idle in transaction

      // - rollback
      // generates noisy notices in server log

      // - begin; rollback;
      // no notices when no active transactions. (good)
      // 'begin' causes notice warning when in transaction. (good)
      // will be not executed as single query when connection is errored in transaction

      // TODO check if query pipelining actually improves perfomance

      // Next query can be already executed and can modify db
      // so destroying connection cannot prevent next query execution.
      // But next query will be executed in explicit transaction
      // so modifications of next query will not be commited automaticaly.
      // Rather then next query does explicit COMMIT
      throw conn.destroy(Error('pooled connection left in transaction'));
    }

    if (this._poolIdleTimeout && !conn.pending /* is idle */) {
      conn._poolTimeoutId = setTimeout(this._onConnectionTimeout, this._poolIdleTimeout, this, conn);
    }
  }
  _onConnectionTimeout(_self, conn) {
    conn.end();
  }
  _onConnectionEnded(conn) {
    this._connections.delete(conn);
  }
  /** exports kErrorPgcode for cases when Pool is injected as dependency
   * and we dont want to import pgwire directly for error handling code */
  get kErrorPgcode() {
    return kErrorPgcode;
  }
}

class Connection {
  constructor({ hostname, port, password, '.debug': debug, ...startupOptions }) {
    this._connectOptions = { hostname, port };
    this._user = startupOptions['user'];
    this._password = password;
    this._debug = ['true', 'on', '1', 1, true].includes(debug);
    this._socket = null;
    this._backendKeyData = null;
    this._parameters = Object.create(null);
    this._lastErrorResponse = null;
    this._responseRxs = /** @type {Channel[]} */ [];
    this._notificationSubscriptions = /** @type {Set<Channel>} */ new Set();
    this._transactionStatus = null;
    this._copyingOut = false;
    this._tx = new Channel();
    this._txReadable = this._tx; // _tx can be nulled after _tx.end(), but _txReadable will be available
    this._startTx = new Channel();
    this._resolveReady = null;
    this._rejectReady = null;
    this._whenReady = new Promise(this._whenReadyExecutor.bind(this));
    this._whenReady.connection = this;
    this._whenReady.catch(warnError);
    this._saslClientFirstMessageBare = null;
    this._whenDestroyed = this._startup(startupOptions); // run background message processing
  }
  _whenReadyExecutor(resolve, reject) {
    // If ready is resolved then it still can be rejected.
    // When ready is rejected then it cannot be resolved or rejected again.
    this._resolveReady = val => {
      this._resolveReady = Boolean; // noop
      this._rejectReady = err => {
        this._whenReady = Promise.reject(err);
        this._whenReady.catch(warnError);
        this._rejectReady = Boolean; // noop
      };
      resolve(val);
    };
    this._rejectReady = err => {
      this._resolveReady = Boolean; // noop
      this._rejectReady = Boolean; // noop
      reject(err);
    };
  }
  /** resolved when connection is established and authenticated */
  get whenReady() {
    return this._whenReady;
  }
  /** resolved when no quieries can be emitted and connection is about to terminate */
  get whenEnded() {
    return this._txReadable.whenEnded;
  }
  /** number of pending queries */
  get pending() {
    return this._responseRxs.length;
  }
  get inTransaction() {
    return this._socket && ( // if not destroyed
      this._transactionStatus == 0x45 || // E
      this._transactionStatus == 0x54 // T
    );
  }
  // TODO accept abortSignal
  query(...args) {
    return new Response(this._queryIter(...args));
  }
  async * _queryIter(...args) {
    if (!this._tx) {
      // TODO call stack
      throw this._destroyReason || Error('cannot query on ended connection');
    }
    // TODO ordered queue .query() and .end() calls
    // .query after .end should throw stable error ? what if auth failed after .end

    const stdinAbortCtl = new AbortController();
    const readyForQueryLock = new Channel();
    const feMessages = Array.from( // materialize messages to ensure no errors during emiting query
      typeof args[0] == 'string'
      ? simpleQuery(args[0], args[1], stdinAbortCtl.signal, readyForQueryLock)
      : extendedQuery(args, stdinAbortCtl.signal)
    );
    const responseRx = new Channel();

    for (const m of feMessages) {
      this._tx.push(m);
    }
    this._responseRxs.push(responseRx);

    try {
      // TODO отмена через .return() не сработает пока текущий неотработавший .next() не завершится,
      // а .next() может долго висеть и ничего не отдавать.
      // Возможно стоит раз в секунду отдавать в канал пустое сообщение
      // при простое со стороны постгреса.
      // Если делать через AbortSignal то тогда висячий .next() придется reject'ить
      // а пользовательский код должен обрабатывать ошибку - неудобно
      const errorResponse = yield * responseRx;
      // но ведь ошибки бывают еще и сетевые, у них свой call stack
      if (errorResponse) {
        throw new PgError(errorResponse);
      }
    } finally {
      // if query completed successfully then all stdins are drained,
      // so abort will have no effect and will not break anything.
      // Оtherwise
      // - if error occured
      // - or iter was .returned()
      // - or no COPY FROM STDIN was queries
      // then we should abort all pending stdins
      stdinAbortCtl.abort();

      // https://github.com/kagis/pgwire/issues/17
      // going to do CancelRequest if response is not ended and there are no other pending responses
      if (this._responseRxs[0] == responseRx && this._responseRxs.length == 1) {
        // new queries should not be emitted during CancelRequest if _tx is not ended
        this._tx?.push(readyForQueryLock);
        // TODO check if frontend messages reached postgres and query is actually executing,
        // check any response messages was received before calling CancelRequest.
        // First messages should be received fast
        // но если мы до сюда дошли значит один yield уже отработал и
        // как минимум первое сообщение уже было получено от сервера
        // так как return() отработает только после next()
        await this._cancelRequest().catch(warnError);
      }
      await responseRx.whenEnded; // skip until ReadyForQuery
      readyForQueryLock.end();
    }
  }

  // should terminate connection gracefully
  // if termination was not gracefull then should throw error ?

  // TODO should be idempotent or throw if already ended ?

  /** terminates connection gracesfully if possible and waits until termination complete */
  async end() {
    // if (!this._closed) {
    //   this._closed = true;
    //   this._resolveClosed();
    // TODO end() should immediatly prevent new queries
    // FIXME delays whenEnded resolution
    // await this._whenReady;
    if (this._tx) {
      // TODO
      // if (pgbouncer) {
      //   const lock = new Channel();
      //   this._whenIdle.then(_ => lock.end());
      //   this._tx.push(lock);
      // }
      this._tx.push(new Terminate());
      this._tx.end();
      this._tx = null;
    }
    this._rejectReady(Error('connection ended'));

    // }
    // TODO destroy will cause error
    await this._whenDestroyed.catch(warnError); // TODO ignore error
  }
  destroy(destroyReason) {
    this._rejectReady(destroyReason || Error('connection destroyed'));
    if (this._socket) {
      _networking.close(this._socket);
      this._socket = null;
    }
    if (this._startTx) {
      this._startTx.end();
      this._startTx = null;
    }
    if (this._tx) {
      this._tx.end();
      this._tx = null;
      // keep destroyReason only if .destroy() called before .end()
      // so new queries will be rejected with stable error.
      this._destroyReason = destroyReason;
    }
    if (this._notificationSubscriptions) {
      this._notificationSubscriptions.forEach(it => void it.end(destroyReason));
      this._notificationSubscriptions = null;
    }
    const responseEndReason = destroyReason || Error('incomplete response');
    while (this._responseRxs.length) {
      this._responseRxs.shift().end(responseEndReason);
    }
    // TODO do CancelRequest to wake stuck connection which can continue executing ?
    return destroyReason; // user code can call .destroy and throw destroyReason in one line
  }
  async * notifications() {
    if (!this._notificationSubscriptions) {
      // TODO call stack
      throw this._destroyReason || Error('cannot receive notifications on destroyed connection');
    }
    const subscriptions = this._notificationSubscriptions;
    const nsub = new Channel();
    subscriptions.add(nsub);
    try {
      yield * nsub;
    } finally {
      subscriptions.delete(nsub);
    }
  }
  logicalReplication () {
    // TODO implement
    // Promise<AsyncIterator> ? wait for CopyBothResponse
  }
  async _cancelRequest() {
    // await this._whenReady; // wait for backendkey
    if (!this._backendKeyData) {
      throw Error('trying to cancel before BackendKeyData received');
    }
    const socket = await this._createSocket();
    try {
      await writeAll(socket, serializeFrontendMessage(
        new CancelRequest(this._backendKeyData),
      ));
    } finally {
      _networking.close(socket);
    }
  }
  async _createSocket() {
    let socket = await _networking.connect(this._connectOptions);
    if (this._tls) {
      try {
        await writeAll(socket, serializeFrontendMessage(new SSLRequest()));
        const sslResp = await readByte(socket);
        if (sslResp == 'S') {
          socket = await Deno.startTls(socket, { });
        }
      } catch (err) {
        _networking.close(socket);
        throw err;
      }
    }
    return socket;
  }
  async _startup(startupOptions) {
    let caughtError;
    try {
      this._socket = await this._createSocket();
      this._startTx.push(new StartupMessage(startupOptions));
      await Promise.all([ // allSettled ?
        // TODO при досрочном завершении одной из функций как будем завершать вторую ?
        // _sendMessages надо завершать потому что она может пайпить stdin,
        // а недокушаный stdin надо прибить

        // когда может обломаться recvMessages
        // - сервер закрыл сокет
        // - сервер прислал херню
        // - pgwire не смог обработать авторизационные сообщения
        this._recvMessages(),
        this._sendMessage(this._startTx),
      ]);
      if (this._lastErrorResponse) {
        throw new PgError(this._lastErrorResponse);
      }
    } catch (err) {
      caughtError = err;
    }
    this.destroy(caughtError);
  }
  async _sendMessage(m) {
    if (m[Symbol.asyncIterator]) {
      const iter = m[Symbol.asyncIterator]();
      // pipe messages
      let value, done;
      try {
        while (!done) {
          [{ value, done }] = await Promise.all([
            iter.next(),
            value && this._sendMessage(value),
          ]);
        }
      } finally {
        await iter.return();
      }
    } else {
      if (this._debug) {
        console.log(... m.payload === undefined
          ? ['<--- %s', m.constructor.name]
          : ['<--- %s %o', m.constructor.name, m.payload],
        );
      }
      // TODO serializeFrontendMessage creates new Uint8Array
      // per every call. Should reuse buffer.
      // And should send all messages of query in single writeAll call
      // (except copy from stdin)
      // TODO zero copy for stdin
      await writeAll(this._socket, serializeFrontendMessage(m));
    }
  }
  async _recvMessages() {
    for await (const m of iterBackendMessages(this._socket)) {
      if (this._debug) {
        console.log(... m.payload === undefined
          ? ['-> %s', m.tag]
          : ['-> %s %o', m.tag, m.payload],
        );
      }
      // TODO check if connection is destroyed to prevent errors in _recvMessage?
      // iterBackendMessages can be in the middle of yield loop over buffered messages
      // and will stopped only when socket.read reached
      await this._recvMessage(m);
    }
  }
  _recvMessage(m) {
    switch (m.tag) {
      case 'DataRow': return this._recvDataRow(m, [m.payload]);
      case 'CopyData': return this._recvCopyData(m, [m.payload]);
      case 'CopyInResponse': return this._recvCopyInResponse(m, m.payload);
      case 'CopyOutResponse': return this._recvCopyOutResponse(m, m.payload);
      case 'CopyBothResponse': return this._recvCopyBothResponse(m, m.payload);
      case 'CopyDone': return this._recvCopyDone(m, m.payload);
      case 'RowDescription': return this._recvRowDescription(m, m.payload);
      case 'NoData': return this._recvNoData(m, m.payload);
      case 'PortalSuspended': return this._recvPortalSuspended(m, m.payload);
      case 'CommandComplete': return this._recvCommandComplete(m, m.payload);
      case 'EmptyQueryResponse': return this._recvEmptyQueryResponse(m, m.payload);

      case 'AuthenticationMD5Password': return this._recvAuthenticationMD5Password(m, m.payload);
      case 'AuthenticationCleartextPassword': return this._recvAuthenticationCleartextPassword(m, m.payload);
      case 'AuthenticationSASL': return this._recvAuthenticationSASL(m, m.payload);
      case 'AuthenticationSASLContinue': return this._recvAuthenticationSASLContinue(m, m.payload);
      case 'AuthenticationOk': return this._recvAuthenticationOk(m, m.payload);
      case 'ParameterStatus': return this._recvParameterStatus(m, m.payload);
      case 'BackendKeyData': return this._recvBackendKeyData(m, m.payload);
      case 'NoticeResponse': return this._recvNoticeResponse(m, m.payload);
      case 'ErrorResponse': return this._recvErrorResponse(m, m.payload);
      case 'ReadyForQuery': return this._recvReadyForQuery(m, m.payload);

      case 'NotificationResponse': return this._recvNotificationResponse(m.payload, m);
    }
  }
  async _recvDataRow(_, /** @type {Array<Array<Uint8Array>>} */ rows) {
    for (const row of rows) {
      for (let i = 0; i < this._rowDecoder.length; i++) {
        const val = row[i];
        if (val != null) {
          row[i] = this._rowDecoder[i](val);
        }
      }
    }
    const [responseRx] = this._responseRxs;
    await responseRx.push(Object.assign(new Uint8Array(0), { rows }));
  }
  async _recvCopyData(_, /** @type {Array<Uint8Array>} */ datas) {
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
    if (!this._copyingOut) {
      return;
    }

    const [responseRx] = this._responseRxs;
    // response.push(datas);
    // TODO recieve bulk CopyData
    for (const it of datas) {
      await responseRx.push(Object.assign(it, { rows: [it] }));
    }
  }
  _recvCopyInResponse(m) {
    this._fwdBackendMessage(m);
  }
  _recvCopyOutResponse(m) {
    this._copyingOut = true;
    this._fwdBackendMessage(m);
  }
  _recvCopyBothResponse(m) {
    this._copyingOut = true;
    this._fwdBackendMessage(m);
  }
  _recvCopyDone(m) {
    this._copyingOut = false;
    this._fwdBackendMessage(m);
  }
  _recvCommandComplete(m) {
    return this._fwdBackendMessage(m);
  }
  _recvEmptyQueryResponse(m) {
    return this._fwdBackendMessage(m);
  }
  _recvPortalSuspended(m) {
    return this._fwdBackendMessage(m);
  }
  _recvNoData(m) {
    return this._fwdBackendMessage(m);
  }
  _recvRowDescription(m, fields) {
    this._rowDecoder = fields.map(getFieldDecoder);
    return this._fwdBackendMessage(m);
  }
  async _recvAuthenticationCleartextPassword() {
    this._startTx.push(new PasswordMessage(this._password));
  }
  async _recvAuthenticationMD5Password(_, { salt }) {
    // TODO stop if no password
    // if (!password) return this._startuptx.end(Error('no password));
    const a = new Md5().update(this._password).update(this._user).toString();
    const b = 'md5' + new Md5().update(a).update(salt).toString();
    this._startTx.push(new PasswordMessage(b));
  }
  async _recvAuthenticationSASL(_, { mechanism }) {
    if (mechanism != 'SCRAM-SHA-256') {
      // TODO gracefull terminate (send Terminate before socket close)
      throw Error(`unsupported SASL mechanism ${mechanism}`);
    }
    const clientNonce = new Uint8Array(24);
    crypto.getRandomValues(clientNonce);
    const clientNonceB64 = b64encode(clientNonce);
    this._saslClientFirstMessageBare = 'n=,r=' + clientNonceB64;
    this._startTx.push(new SASLInitialResponse({
      mechanism: 'SCRAM-SHA-256',
      data: utf8encoder.encode('n,,' + this._saslClientFirstMessageBare),
    }));
  }
  async _recvAuthenticationSASLContinue(_, data) {
    const firstServerMessage = utf8decode(data);
    const { i: iterations, s: saltB64, r: nonceB64 } = (
      firstServerMessage
      .split(',')
      .map(it => /^([^=]*)=(.*)$/.exec(it))
      .reduce((acc, [, key, val]) => ({ ...acc, [key]: val }), {})
    );
    const finalMessageWithoutProof = 'c=biws,r=' + nonceB64;
    const salt = b64decode(saltB64);
    const passwordUtf8 = utf8encoder.encode(this._password.normalize());

    const saltedPassword = new Uint8Array(await crypto.subtle.deriveBits(
      { name: 'PBKDF2', hash: 'SHA-256', salt, iterations },
      await crypto.subtle.importKey('raw', passwordUtf8, 'PBKDF2', false, ['deriveBits']),
      32,
    ));

    // const hmac = (key, inp) => createHmac('sha256', key).update(inp).digest();
    const clientKey = hmac(saltedPassword, 'Client Key');
    const storedKey = createHash('sha256').update(clientKey).digest();
    const authMessage = (
      this._saslClientFirstMessageBare + ',' +
      firstServerMessage + ',' +
      finalMessageWithoutProof
    );
    const clientSignature = hmac(storedKey, authMessage);
    const clientProof = new Uint8Array(clientKey.map((b, i) => b ^ clientSignature[i]));
    const serverKey = hmac(saltedPassword, 'Server Key');
    this._saslServerSignatureB64 = hmac(serverKey, authMessage).toString('base64');
    this._startTx.push(new SASLResponse(utf8encode(
      finalMessageWithoutProof + ',p=' + clientProof.toString('base64'),
    )));

    function hmac(key, inp) {

    }
  }
  async _recvAuthenticationOk() {
    // we dont need password anymore, its more secure to forget it
    this._password = null;
  }

  _recvErrorResponse(_, payload) {
    this._copyingOut = false;
    this._lastErrorResponse = payload;
    // TODO ErrorResponse is associated with query only when followed by ReadyForQuery
    // ErrorResonse can be received when socket closed by server, and .query can be
    // called just before socket closed
    // if (!this._fwdBackendMessage(m)) {
    //   // TODO wait until socket close and check for _lastErrorResponse
    //   // throw new PgError(m.payload);
    // }
  }
  _recvReadyForQuery(m, { transactionStatus }) {
    this._transactionStatus = transactionStatus;
    if (this._fwdBackendMessage(m)) {
      this._responseRxs.shift().end(null, this._lastErrorResponse);
      this._lastErrorResponse = null;
      return;
    }
    // complete startup
    this._startTx.push(this._txReadable);
    this._startTx.end();
    this._startTx = null;
    this._resolveReady();
  }

  async _recvNotificationResponse(_, payload) {
    await Promise.all(Array.from(
      this._notificationSubscriptions,
      nc => nc.push(payload)
    ));
  }
  _recvNoticeResponse(m) {
    // FIXME async NoticeResponse can be recevied after .query called
    // but before query messages actually received by postres.
    // Such NoticeResponse will be uncorrectly forwared to query response.
    // Seems that there is no way to check whether NoticeResponse
    // belongs to query or not. May be we should treat all NoticeResponse
    // messages as connection level notices and do not forward NoticeResponses
    // to query responses at all.

    if (!this._fwdBackendMessage(m)) {
      // NoticeResponse is not associated with query
      // TODO report nonquery NoticeResponse
    }
  }
  _recvParameterStatus(_, { parameter, value }) {
    this._parameters[parameter] = value;
  }
  _recvBackendKeyData(_, backendKeyData) {
    this._backendKeyData = backendKeyData;
  }
  _fwdBackendMessage(m) {
    if (!this._startTx && this._responseRxs.length) {
      const [responseRx] = this._responseRxs;
      responseRx.push(Object.assign(new Uint8Array(0), { rows: [] }, m));
      return true;
    }
  }

  // // TODO replace with { connection, then, catch, finally } object, less magic
  // // sync pgconnect not expected to be commonly used anyway.
  // // the only case is Pool, and this case is internal
  // then(...args) { return this._whenReady.then(...args); }
  // catch(...args) { return this._whenReady.catch(...args); }
  // finally(...args) { return this._whenReady.finally(...args); }
  // _createReadyConn() { return new Proxy(this, { get: this._getPropExceptThen }); }
  // _getPropExceptThen(target, prop) { return prop == 'then' ? undefined : target[prop]; }
}

// there is no strong need to use generators to create message sequence,
// but generators help to generate conditional messages in more readable way
function * simpleQuery(script, { stdins = [] } = {}, stdinAbortSignal, readyForQueryLock) {
  yield new Query(script);
  for (const stdin of stdins) {
    yield wrapCopyData(stdin, stdinAbortSignal);
  }
  // when CREATE_REPLICATION_SLOT or START_REPLICATION is emitted
  // then no other queries should be emmited until ReadyForQuery is received.
  // Seems that its a postgres server bug.
  // Looks like fragile dirty hack but its not.
  // Its dirty but safe enough because no comments or other statements can
  // precede CREATE_REPLICATION_SLOT or START_REPLICATION
  if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(script)) {
    yield readyForQueryLock;
    return;
  }
  yield new CopyFail('missing copy upstream');
}
function * extendedQuery(blocks, stdinAbortSignal) {
  for (const m of blocks) {
    yield * extendedQueryBlock(m, stdinAbortSignal);
  }
  yield new Sync();
}
function extendedQueryBlock(m, stdinAbortSignal) {
  switch (m.message) {
    case undefined:
    case null: return extendedQueryStatement(m, stdinAbortSignal);
    case 'Parse': return extendedQueryParse(m);
    case 'Bind': return extendedQueryBind(m);
    case 'Execute': return extendedQueryExecute(m, stdinAbortSignal);
    case 'DescribeStatement': return extendedQueryDescribeStatement(m);
    case 'CloseStatement': return extendedQueryCloseStatement(m);
    case 'DescribePortal': return extendedQueryDescribePortal(m);
    case 'ClosePortal': return extendedQueryClosePortal(m);
    default: throw Error('unknown extended message ' + JSON.stringify(m.message));
  }
}
function * extendedQueryStatement({ statement, params, limit, stdin, noBuffer }, stdinAbortSignal) {
  const paramTypes = params?.map(({ type }) => type);
  const flush = []; // [new Flush()];
  yield * extendedQueryParse({ statement, paramTypes });
  yield * flush;
  yield * extendedQueryBind({ params });
  yield * flush;
  yield * extendedQueryExecute({ limit, stdin }, stdinAbortSignal);
  yield * flush;
}
function * extendedQueryParse({ statement, statementName, paramTypes }) {
  yield new Parse({ statement, statementName, paramTypes: paramTypes?.map(normalizeTypeid) });
}
// FIXME rename ugly outFormats0t1b
function * extendedQueryBind({ portal, statementName, outFormats0t1b = [1], params }) {
  yield new Bind({ portal, statementName, outFormats0t1b, params: params?.map(encodeParam) });
}
function * extendedQueryExecute({ portal, stdin, limit }, stdinAbortSignal) {
  // TODO write test to explain why
  // we need unconditional DescribePortal
  // before Execute
  yield new DescribePortal(portal);
  // TODO nobuffer option
  // yield new Flush();
  yield new Execute({ portal, limit });
  if (stdin) {
    yield wrapCopyData(stdin, stdinAbortSignal)
  } else {
    // CopyFail message ignored by postgres
    // if there is no COPY FROM statement
    yield new CopyFail('missing copy upstream');
  }
  // TODO nobuffer option
  // yield new Flush();
}
function * extendedQueryDescribeStatement({ statementName }) {
  yield new DescribeStatement(statementName);
}
function * extendedQueryCloseStatement({ statementName }) {
  yield new CloseStatement(statementName);
}
function * extendedQueryDescribePortal({ portal }) {
  yield new DescribePortal(portal);
}
function * extendedQueryClosePortal({ portal }) {
  yield new ClosePortal(portal);
}
async function * wrapCopyData(source, abortSignal) {
  // TODO dry
  // if (abortSignal.aborted) {
  //   return;
  // }
  try {
    for await (const chunk of source) {
      yield new CopyData(chunk);
      if (abortSignal.aborted) {
        yield new CopyFail('aborted'); // TODO is correct?
        return;
      }
    }
  } catch (err) {
    // FIXME err.stack lost
    // store err
    // do CopyFail (copy_error_key)
    // rethrow stored err when ErrorResponse received
    yield new CopyFail(String(err));
    return;
  }
  yield new CopyDone();
}

class Response {
  _loadPromise
  _iter

  constructor(iter) {
    this._iter = iter;
  }
  [Symbol.asyncIterator]() {
    // TODO activate iterator ? emit query
    return this._iter[Symbol.asyncIterator]();
  }
  then(...args) {
    return this._load().then(...args);
  }
  catch(...args) {
    return this._load().catch(...args);
  }
  finally(...args) {
    return this._load().finally(...args);
  }
  async _load(noCache) {
    if (!noCache) {
      return this._loadPromise || (this._loadPromise = this._load(true));
    }
    // let inTransaction = false;
    let lastResult;
    const results = [];
    for await (const chunk of this) {
      lastResult = lastResult || {
        rows: [],
        // TODO copied: [],
        notices: [],
        scalar: undefined,
        command: undefined,
        suspended: false,
        empty: false,
      };
      lastResult.rows.push(...chunk.rows);
      switch (chunk.tag) {
        // TODO RowDescription
        // TODO CopyOutResponse

        case 'NoticeResponse':
          lastResult.notices.push(chunk.payload);
          continue;
        // case 'ReadyForQuery':
        //   inTransaction = chunk.payload.transactionStatus != 0x49 /*I*/;
        //   continue;
        default:
          continue;
        case 'CommandComplete':
          lastResult.command = chunk.payload;
          break;
        case 'PortalSuspended':
          lastResult.suspended = true;
          break;
        case 'EmptyQueryResponse':
          lastResult.empty = true;
          break;
      }
      lastResult.scalar = lastResult.rows[0];
      // TODO seems that `scalar` has no use cases for COPY TO STDOUT
      if (Array.isArray(lastResult.scalar)) { // DataRow or CopyData
        lastResult.scalar = lastResult.scalar[0];
      }
      results.push(lastResult);
      lastResult = null;
    }
    return {
      ...results[results.length - 1],
      // inTransaction,
      results,
      // TODO root `rows` concat ?
      get notices() {
        return results.flatMap(({ notices }) => notices);
      },
    };
  }
}

async function * iterBackendMessages(reader) {
  let buf = new Uint8Array(16_640);
  let nacc = 0;
  for (;;) {
    if (nacc >= buf.length) { // grow buffer
      const oldbuf = buf;
      buf = new Uint8Array(oldbuf.length * 2); // TODO prevent uncontrolled grow
      buf.set(oldbuf);
    }
    const nread = await reader.read(buf.subarray(nacc));
    if (nread == null) break;
    nacc += nread;

    let nparsed = 0;
    for (;;) {
      const itag = nparsed;
      const isize = itag + 1;
      const ipayload = isize + 4;
      if (nacc < ipayload) break; // incomplete message
      // const size = (buf[isize] << 24) | (buf[isize + 1] << 16) | (buf[isize + 2] << 8) | buf[isize + 3];
      const size = new DataView(buf.buffer, isize).getInt32();
      if (size < 4) {
        throw Error('invalid backend message size');
      }
      const inext = isize + size;
      if (nacc < inext) break; // incomplete message
      const message = parseBackendMessage(buf[itag], buf.subarray(ipayload, inext));
      nparsed = inext;
      yield message; // TODO batch DataRow and CopyData
    }

    if (nparsed) { // TODO check if copyWithin(0, 0) is noop
      buf.copyWithin(0, nparsed, nacc); // move unconsumed bytes to begining of buffer
      nacc -= nparsed;
    }
  }
}

function parseBackendMessage(/** @type {number} */ asciiTag, /** @type {Uint8Array} */ buf) {
  let pos = 0;

  function readInt32BE() {
    return (buf[pos++] << 24) | (buf[pos++] << 16) | (buf[pos++] << 8) | buf[pos++]
  }
  function readInt16BE() {
    return (buf[pos++] << 8) | buf[pos++]
  }
  function readUint8() {
    return buf[pos++]
  }
  function readBytes(n) {
    return buf.subarray(pos, pos += n);
  }
  function readToEnd() {
    return buf.subarray(pos);
    // TODO pos += ?
  }
  function readString() {
    const endIdx = buf.indexOf(0x00, pos);
    if (endIdx < 0) {
      throw Error('unexpected end of message');
    }
    const strbuf = buf.subarray(pos, endIdx);
    pos = endIdx + 1;
    return utf8decoder.decode(strbuf);
  }

  switch (asciiTag) {
    case 0x64 /* d */: return {
      tag: 'CopyData',
      payload: buf, // FIXME create buf copy ?
    };
    case 0x44 /* D */: {
      const nfields = readInt16BE()
      const row = Array(nfields);
      for (let i = 0; i < nfields; i++) {
        const len = readInt32BE();
        row[i] = len < 0 ? null : readBytes(len);
      }
      return {
        tag: 'DataRow',
        payload: row,
      };
    }
    case 0x52 /* R */:
      switch (readInt32BE()) {
        case 0: return {
          tag: 'AuthenticationOk',
        };
        case 2: return {
          tag: 'AuthenticationKerberosV5',
        };
        case 3: return {
          tag: 'AuthenticationCleartextPassword',
        };
        case 5: return {
          tag: 'AuthenticationMD5Password',
          payload: { salt: readBytes(4) },
        };
        case 6: return {
          tag: 'AuthenticationSCMCredential',
        };
        case 7: return {
          tag: 'AuthenticationGSS',
        };
        case 8: return {
          tag: 'AuthenticationGSSContinue',
          payload: readToEnd(),
        };
        case 9: return {
          tag: 'AuthenticationSSPI',
        };
        case 10: return {
          tag: 'AuthenticationSASL',
          payload: { mechanism: readString() },
        };
        case 11: return {
          tag: 'AuthenticationSASLContinue',
          payload: readToEnd(),
        };
        case 12: return {
          tag: 'AuthenticationSASLFinal',
          payload: readToEnd(),
        };
        default: throw Error('unknown auth message');
      }
    case 0x76 /* v */: return {
      tag: 'NegotiateProtocolVersion',
      payload: {
        version: readInt32BE(),
        unrecognizedOptions: Array.from(
          { length: readInt32BE() },
          readString,
        ),
      },
    };
    case 0x53 /* S */: return {
      tag: 'ParameterStatus',
      payload: {
        parameter: readString(),
        value: readString(),
      },
    };
    case 0x4b /* K */: return {
      tag: 'BackendKeyData',
      payload: {
        pid: readInt32BE(),
        secretKey: readInt32BE(),
      },
    };
    case 0x5a /* Z */: return {
      tag: 'ReadyForQuery',
      payload: {
        transactionStatus: readUint8(),
      },
    };
    case 0x48 /* H */: return {
      tag: 'CopyOutResponse',
      payload: readCopyResp(),
    };
    case 0x47 /* G */: return {
      tag: 'CopyInResponse',
      payload: readCopyResp(),
    };
    case 0x57 /* W */: return {
      tag: 'CopyBothResponse',
      payload: readCopyResp(),
    };
    case 0x63 /* c */: return {
      tag: 'CopyDone',
    };
    case 0x54 /* T */: return {
      tag: 'RowDescription',
      payload: Array.from({ length: readInt16BE() }, _ => ({
        name: readString(),
        tableid: readInt32BE(),
        column: readInt16BE(),
        typeid: readInt32BE(),
        typelen: readInt16BE(),
        typemod: readInt32BE(),
        binary: readInt16BE(),
      })),
    };
    case 0x74 /* t */: return {
      tag: 'ParameterDescription',
      payload: Array.from(
        { length: readInt16BE() },
        _ => readInt32BE(),
      ),
    };
    case 0x43 /* C */: return {
      tag: 'CommandComplete',
      payload: readString(),
    };
    case 0x31 /* 1 */: return {
      tag: 'ParseComplete',
    };
    case 0x32 /* 2 */: return {
      tag: 'BindComplete',
    };
    case 0x33 /* 3 */: return {
      tag: 'CloseComplete',
    };
    case 0x73 /* s */: return {
      tag: 'PortalSuspended',
    };
    case 0x49 /* I */: return {
      tag: 'EmptyQueryResponse',
    };
    case 0x4e /* N */: return {
      tag: 'NoticeResponse',
      payload: readErrorOrNotice(),
    };
    case 0x45 /* E */: return {
      tag: 'ErrorResponse',
      payload: readErrorOrNotice(),
    };
    case 0x6e /* n */: return {
      tag: 'NoData',
    };
    case 0x41 /* A */: return {
      tag: 'NotificationResponse',
      payload: {
        pid: readInt32BE(),
        channel: readString(),
        payload: readString(),
      },
    };
    default: return {
      tag,
      payload,
    };
  }

  function readErrorOrNotice() {
    const fields = Array(256);
    for (;;) {
      const fieldCode = readUint8();
      if (!fieldCode) break;
      fields[fieldCode] = readString();
    }
    return {
      severity: fields[0x53], // S
      code: fields[0x43], //  C
      message: fields[0x4d], // M
      detail: fields[0x44], // D
      hint: fields[0x48], // H
      position: fields[0x50] && Number(fields[0x50]), // P
      internalPosition: fields[0x70] && Number(fields[0x70]), // p
      internalQuery: fields[0x71], // q
      where: fields[0x57], // W
      file: fields[0x46], // F
      line: fields[0x4c] && Number(fields[0x4c]), // L
      routine: fields[0x52], // R
      schema: fields[0x73], // s
      table: fields[0x74], // t
      column: fields[0x63], // c
      datatype: fields[0x64], // d
      constraint: fields[0x6e], // n
    };
  }

  function readCopyResp() {
    return {
      binary: readUint8(),
      binaryPerAttr: Array.from(
        { length: readInt16BE() },
        readInt16BE,
      ),
    };
  }
}


function serializeFrontendMessage(message) {
  const counter = new CounterWriter();
  message.write(counter, NaN, message.payload);
  // FIXME reuse buffer
  const bytes = new Uint8Array(counter.result);
  message.write(new BufferWriter(bytes.buffer), counter.result, message.payload);
  return bytes;
}

class FrontendMessage {
  constructor(payload) {
    this.payload = payload;
  }
}

class StartupMessage extends FrontendMessage {
  write(w, size, options) {
    w.writeInt32BE(size);
    w.writeInt32BE(0x00030000);
    for (const [key, val] of Object.entries(options)) {
      w.writeString(key);
      w.writeString(String(val));
    }
    w.writeUint8(0);
  }
}

class CancelRequest extends FrontendMessage {
  write(w, _size, { pid, secretKey }) {
    w.writeInt32BE(16);
    w.writeInt32BE(80877102); // (1234 << 16) | 5678
    w.writeInt32BE(pid);
    w.writeInt32BE(secretKey);
  }
}

class SSLRequest extends FrontendMessage {
  write(w) {
    w.writeInt32BE(8);
    w.writeInt32BE(80877102); // (1234 << 16) | 5678
  }
}

class PasswordMessage extends FrontendMessage {
  write(w, size, payload) {
    w.writeUint8(0x70); // p
    w.writeInt32BE(size - 1);
    w.writeString(payload);
  }
}

class SASLInitialResponse extends FrontendMessage {
  write(w, size, { mechanism, data }) {
    w.writeUint8(0x70); // p
    w.writeInt32BE(size - 1);
    w.writeString(mechanism);
    if (data) {
      w.writeInt32BE(data.byteLength);
      w.write(data);
    } else {
      w.writeInt32BE(-1);
    }
  }
}

class SASLResponse extends FrontendMessage {
  write(w, size, data) {
    w.writeUint8(0x70); // p
    w.writeInt32BE(size - 1);
    w.write(data);
  }
}

class Query extends FrontendMessage {
  write(w, size) {
    w.writeUint8(0x51); // Q
    w.writeInt32BE(size - 1);
    w.writeString(this.payload);
  }
}

class Parse extends FrontendMessage {
  write(w, size, { statement, statementName = '', paramTypes = [] }) {
    w.writeUint8(0x50); // P
    w.writeInt32BE(size - 1);
    w.writeString(statementName);
    w.writeString(statement);
    w.writeInt16BE(paramTypes.length);
    for (const typeid of paramTypes) {
      w.writeUint32BE(typeid || 0);
    }
  }
}

class Bind extends FrontendMessage {
  write(w, size, { portal = '', statementName = '', params = [], outFormats0t1b = [] }) {
    w.writeUint8(0x42); // B
    w.writeInt32BE(size - 1);
    w.writeString(portal);
    w.writeString(statementName);
    w.writeInt16BE(params.length);
    for (const p of params) {
      w.writeInt16BE(Number(p instanceof Uint8Array));
    }
    w.writeInt16BE(params.length);
    for (const p of params) {
      if (p == null) {
        w.writeInt32BE(-1);
        continue;
      }
      let encoded = p;
      // fixme avoid twice encoding
      if (!(p instanceof Uint8Array)) {
        encoded = new TextEncoder().encode(p);
      }
      w.writeInt32BE(encoded.length);
      w.write(encoded);
    }
    w.writeInt16BE(outFormats0t1b.length);
    for (const fmt of outFormats0t1b) {
      w.writeInt16BE(fmt);
    }
  }
}

class Execute extends FrontendMessage {
  write(w, size, { portal = '', limit = 0 }) {
    w.writeUint8(0x45); // E
    w.writeInt32BE(size - 1);
    w.writeString(portal);
    w.writeUint32BE(limit);
  }
}

class DescribeStatement extends FrontendMessage {
  write(w, size, statementName = '') {
    w.writeUint8(0x44); // D
    w.writeInt32BE(size - 1);
    w.writeUint8(0x53); // S
    w.writeString(statementName);
  }
}

class DescribePortal extends FrontendMessage {
  write(w, size, portal = '') {
    w.writeUint8(0x44); // D
    w.writeInt32BE(size - 1);
    w.writeUint8(0x50); // P
    w.writeString(portal);
  }
}

class ClosePortal extends FrontendMessage {
  write(w, size, portal = '') {
    w.writeUint8(2); // C
    w.writeInt32BE(size - 1);
    w.writeUint8(0x50); // P
    w.writeString(portal);
  }
}

class CloseStatement extends FrontendMessage {
  write(w, size, statementName = '') {
    w.writeUint8(2); // C
    w.writeInt32BE(size - 1);
    w.writeUint8(0x53); // S
    w.writeString(statementName);
  }
}

class Sync extends FrontendMessage {
  write(w) {
    w.writeUint8(0x53); // S
    w.writeInt32BE(4);
  }
}

// unused
class Flush extends FrontendMessage {
  write(w) {
    w.writeUint8(0x48); // H
    w.writeInt32BE(4);
  }
}

class CopyData extends FrontendMessage {
  write(w, size, data) {
    w.writeUint8(0x64); // d
    w.writeInt32BE(size - 1);
    w.write(data);
  }
}

class CopyDone extends FrontendMessage {
  write(w) {
    w.writeUint8(0x63); // c
    w.writeInt32BE(4);
  }
}

class CopyFail extends FrontendMessage {
  write(w, size, cause) {
    w.writeUint8(0x66); // f
    w.writeInt32BE(size - 1);
    w.writeString(cause);
  }
}

class Terminate extends FrontendMessage {
  write(w) {
    w.writeUint8(0x58); // X
    w.writeInt32BE(4);
  }
}

// class DataReader {
//   _view
//   _pos = 0
//   constructor(/** @type {DataView} */ view) {
//     this._view = view;
//   }
//   readUint8() {
//     return this._view.getUint8(this._move(1));
//   }
//   readInt16BE() {
//     return this._view.getUint16(this._move(2));
//   }
//   readInt32BE() {
//     return this._view.getInt32(this._move(4));
//   }
//   readExact(size) {
//     return new Uint8Array(
//       this._view.buffer,
//       this._view.byteOffset + this._move(this._pos, size),
//       size,
//     );
//   }
//   readString() {
//     const arr = new Uint8Array(
//       this._view.buffer,
//       this._view.byteOffset + this._move(this._pos, size),
//     );
//     const endIdx = arr.indexOf(0);
//     if (endIdx < 0) {
//       throw Error('unexpected EOF');
//     }
//     this._pos += endIdx + 1;
//     return utf8decoder.decode(arr.subarray(0, endIdx));
//   }
//   _move(size) {
//     const pos = this._pos;
//     this._pos += size;
//     return pos;
//   }
// }


class BufferWriter {
  _buf
  _pos = 0
  constructor(buf) {
    this._buf = buf
  }
  writeUint8(val) {
    new DataView(this._buf, this._pos).setUint8(0, val);
    this._pos++;
  }
  writeInt16BE(val) {
    new DataView(this._buf, this._pos).setInt16(0, val);
    this._pos += 2;
  }
  writeInt32BE(val) {
    new DataView(this._buf, this._pos).setInt32(0, val);
    this._pos += 4
  }
  writeUint32BE(val) {
    new DataView(this._buf, this._pos).setUint32(0, val);
    this._pos += 4
  }
  writeString(val) {
    const { written } = utf8encoder.encodeInto(val, new Uint8Array(this._buf, this._pos));
    this._pos += written;
    this.writeUint8(0);
  }
  write(val) {
    if (!(val instanceof Uint8Array)) {
      throw TypeError('Uint8Array expected');
    }
    new Uint8Array(this._buf, this._pos).set(val);
    this._pos += val.byteLength;
  }
}

class CounterWriter {
  result = 0
  writeUint8() {
    this.result += 1;
  }
  writeInt16BE() {
    this.result += 2;
  }
  writeInt32BE() {
    this.result += 4;
  }
  writeUint32BE() {
    this.result += 4;
  }
  writeString(val) {
    // FIXME mem inefficient
    this.write(utf8encoder.encode(val));
    this.writeUint8(0);
  }
  write(val) {
    if (!(val instanceof Uint8Array)) {
      throw TypeError('Uint8Array expected');
    }
    this.result += val.byteLength;
  }
}


function normalizeTypeid(typeidOrName) {
  if (typeof typeidOrName == 'number') {
    return typeidOrName;
  }
  const typeid = pgtypes.get(typeidOrName)?.id;
  if (!typeid) {
    throw Error('unknown builtin type name ' + JSON.stringify(typeidOrName));
  }
  return typeid;
}
function encodeParam({ type, value }) {
  const pgtypeProvider = pgtypes.get(type);
  return pgtypeProvider ? pgtypeProvider.encode(value) : value;
}
function getFieldDecoder({ typeid, binary }) {
  const { decodeBin = arrcopy, decodeText = utf8decode } = pgtypes.get(typeid) || {};
  return binary ? decodeBin : decodeText;
  function arrcopy(arr) {
    return arr.slice();
  }
}

/**
 * @callback decodeBin
 * @param {Uint8Array} bytes
 */

/**
 * @callback decodeText
 * @param {Uint8Array} bytes
 */

/**
 * @callback encodeBin
 * @param {Uint8Array} bytes
 * @returns {Uint8Array}
 */

// TODO avoid metaprogramming ?

/**
 * @type {Map<(string|number), {
 *   id: number,
 *   decodeBin: decodeBin,
 *   decodeText: decodeText,
 *   encode
 * }>}
 */
 const pgtypes = [{
  name: 'bool',
  id: 16,
  arrayid: 1000,
  decodeBin: b => b[0] == 1,
  decodeText: b => b[0] == 0x74, // t
  encode: val => val ? 't' : 'f',
}, {
  // TODO bytea_output escape
  // https://www.postgresql.org/docs/9.6/datatype-binary.html#AEN5830
  name: 'bytea',
  id: 17,
  arrayid: 1001,
  decodeBin: b => b.slice(),
  decodeText: s => hexDecode(s.subarray(2 /* skip \x */)),
  encode: val => val,
  encodeBin: true,
}, {
  name: 'int8',
  id: 20,
  arrayid: 1016,
  decodeBin: b => new DataView(b.buffer, b.byteOffset, b.byteLength).getBigInt64(),
  decodeText: s => BigInt(utf8decode(s)),
  encode: String,
}, {
  name: 'int2',
  id: 21,
  arrayid: 1005,
  decodeBin: b => new DataView(b.buffer, b.byteOffset, b.byteLength).getInt16(),
  decodeText: s => Number(utf8decode(s)),
  encode: String,
}, {
  name: 'int4',
  id: 23,
  arrayid: 1007,
  decodeBin: b => new DataView(b.buffer, b.byteOffset, b.byteLength).getInt32(),
  decodeText: s => Number(utf8decode(s)),
  encode: String,
}, {
  name: 'float4',
  id: 700,
  arrayid: 1021,
  decodeBin: b => new DataView(b.buffer, b.byteOffset, b.byteLength).getFloat32(),
  decodeText: s => Number(utf8decode(s)),
  encode: String,
}, {
  name: 'float8',
  id: 701,
  arrayid: 1022,
  decodeBin: b => new DataView(b.buffer, b.byteOffset, b.byteLength).getFloat64(),
  decodeText: s => Number(utf8decode(s)),
  encode: String,
}, {
  name: 'text',
  id: 25,
  arrayid: 1009,
  decodeBin: utf8decode,
  decodeText: utf8decode,
  encode: String,
}, {
  name: 'json',
  id: 114,
  arrayid: 199,
  decodeBin: b => JSON.parse(utf8decode(b)),
  decodeText: s => JSON.parse(utf8decode(s)),
  encode: JSON.stringify,
}, {
  name: 'jsonb',
  id: 3802,
  arrayid: 3807,
  decodeBin: b => JSON.parse(utf8decode(b.subarray(1/* skip version byte */))),
  decodeText: s => JSON.parse(utf8decode(s)),
  encode: JSON.stringify,
}, {
  name: 'pg_lsn',
  id: 3220,
  arrayid: 3221,
  decodeBin: bytes => hexEncode(bytes).toUpperCase().replace(/.{8}/, '$&/'),
  decodeText: bytes => utf8decode(bytes).split('/').map(it => it.padStart(8, '0')).join('/'),
  encode: String,
}, {
  name: 'uuid',
  id: 2950,
  arrayid: 2951,
  decodeBin: bytes => hexEncode(bytes).replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5'),
  decodeText: utf8decode,
  encode: String,
}, {
  name: 'varchar',
  id: 1043,
  arrayid: 1015,
  decodeBin: utf8decode,
  decodeText: utf8decode,
  encode: String,
}]

.reduce((map, { name, id, arrayid, decodeBin, decodeText, encode, encodeBin }) => {
  const pgtype = { id, decodeBin, decodeText, encode };
  const pgtypearr = {
    id: arrayid,
    decodeBin: buf => decodeBinArray(buf, decodeBin),
    // FIXME decode -> encode perfomance
    decodeText: bytes => decodeTextArray(utf8decode(bytes), str => decodeText(utf8encoder.encode(str))),
    encode: encodeBin
      ? arr => encodeBinArray(arr, encode, id)
      : arr => encodeTextArray(arr, encode),
  };
  return (
    map
    .set(id, pgtype)
    .set(name, pgtype)
    .set(arrayid, pgtypearr)
    .set(name + '[]', pgtypearr)
  );
}, new Map());

function decodeTextArray(inp, decodeElem) {
  inp = inp.replace(/^\[.+=/, ''); // skip dimensions
  const jsonArray = inp.replace(/{|}|,|"(?:[^"\\]|\\.)*"|[^,}]+/gy, token => (
    token == '{' ? '[' :
    token == '}' ? ']' :
    token == 'NULL' ? 'null' :
    token == ',' || token[0] == '"' ? token :
    JSON.stringify(token)
  ));
  return JSON.parse(
    jsonArray,
    (_, elem) => typeof elem == 'string' ? decodeElem(elem) : elem,
  );
}

/**
 * @param {Uint8Array} bytes
 * @param {decodeBin} decodeElem
 * */
function decodeBinArray(bytes, decodeElem) {
  const ndim = readInt32BE(0);
  let cardinality = 0;
  for (let di = ndim - 1; di >= 0; di--) {
    cardinality += readInt32BE(12 + di * 8);
  }
  let result = Array(cardinality);
  for (let pos = 12 + ndim * 8, i = 0; pos < bytes.byteLength; i++) {
    const len = readInt32BE(pos);
    pos += 4;
    if (len < 0) {
      result[i] = null;
    } else {
      result[i] = decodeElem(bytes.subarray(pos, pos += len));
    }
  }
  for (let di = ndim - 1; di > 0; di--) {
    const dimlen = readInt32BE(12 + di * 8);
    const reshaped = Array(result.length / dimlen);
    for (let i = 0; i < reshaped.length; i++) {
      reshaped[i] = result.slice(i * dimlen, (i + 1) * dimlen);
    }
    result = reshaped;
  }
  return result;

  function readInt32BE(p) {
    return (bytes[p] << 24) | (bytes[p + 1] << 16) | (bytes[p + 2] << 8) | bytes[p + 3]
  }
}

// FIXME: one dimension only
function encodeTextArray(arr, encodeElem) {
  return JSON.stringify(arr, function (_, elem) {
    return this == arr && elem != null ? encodeElem(elem) : elem;
  }).replace(/^\[(.*)]$/, '{$1}');
}

// FIXME: one dimension only
/**
 *
 * @param {Array} array
 * @param {encodeBin} encodeElem
 * @param {number} elemTypeid
 * @returns
 */
function encodeBinArray(array, encodeElem, elemTypeid) {
  const ndim = 1;
  /** @type {Uint8Array[]} */
  const encodedArray = Array(array.length);
  let size = 4 + 4 + 4 + ndim * (4 + 4) + array.length * 4;
  let hasNull = 0;
  for (let i = 0; i < array.length; i++) {
    if (array[i] == null) {
      hasNull = 1;
    } else {
      const elbytes = encodeElem(array[i]);
      size += elbytes.length;
      encodedArray[i] = elbytes;
    }
  }
  const result = new Uint8Array(size);
  const dv = new DataView(result.buffer);
  let pos = 0;
  dv.setInt32(pos, 1), pos += 4;
  dv.setInt32(pos, hasNull), pos += 4;
  dv.setInt32(pos, elemTypeid), pos += 4;
  dv.setInt32(pos, array.length), pos += 4;
  const lb = 1;
  dv.setInt32(pos, lb), pos += 4;
  for (const elbytes of encodedArray) {
    if (elbytes) {
      dv.setInt32(pos, elbytes.byteLength), pos += 4;
      result.set(elbytes, pos), pos += elbytes.byteLength; // FIXME copy
    } else {
      dv.setInt32(pos, -1), pos += 4;
    }
  }
  return result;
}

const utf8decoder = new TextDecoder('utf-8', { fatal: true });
const utf8encoder = new TextEncoder();

function utf8decode(b) {
  return utf8decoder.decode(b);
}


class Channel {
  constructor() {
    this._buf = [];
    this._result = undefined;
    this._error = null;
    this._ended = false;
    this._resolveDrained = _ => _;
    this._resolvePushed = _ => _;
    this.whenEnded = new Promise(resolve => {
      this._resolveEnded = resolve;
    });
    this._whenDrained = null;
    this._resolveDrained = Boolean; // noop
    this._whenDrainedExecutor = resolve => this._resolveDrained = resolve;
    this._iter = this._iterate();
  }

  push(value) {
    if (this._ended) {
      throw Error('push after ended');
    }
    if (!this._buf) {
      return; // iterator is aborted
    }
    if (this._buf.push(value) == 1) {
      this._whenDrained = new Promise(this._whenDrainedExecutor);
    }
    this._resolvePushed();
    return this._whenDrained;
  }
  end(error, result) {
    if (this._ended) {
      throw Error('already ended');
    }
    this._error = error;
    this._result = result;
    this._ended = true;
    this._resolvePushed();
    this._resolveEnded();
  }
  async * _iterate() {
    const whenPushedExecutor = resolve => {
      this._resolvePushed = resolve;
    };
    try {
      for (;;) {
        yield * this._buf; // FIXME can stuck for a long time when push/next phase are not in sync
        this._buf = [];
        this._resolveDrained();
        if (this._error) {
          throw this._error; // TODO rethrow nested to save callstack ?
        }
        if (this._ended) {
          return this._result;
        }
        await new Promise(whenPushedExecutor);
      }
    } finally {
      this._buf = null;
      this._resolveDrained();
    }
  }
  [Symbol.asyncIterator]() {
    return this._iter[Symbol.asyncIterator]();
  }
}

function warnError(err) {
  // console.trace('warning', err);
}


// We will not use Deno std to make this module compatible with Node

function b64encode(/** @type {Uint8Array} */ bytes) {
  return btoa(new TextDecoder().decode(bytes));
}
function b64decode(/** @type {string} */ b64) {
  return new TextEncoder().encode(atob(b64));
}


function hexEncode(/** @type {Uint8Array} */ bytes) {
  return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
}
function hexDecode(hex) {
  return Uint8Array.from(
    { length: hex.length / 2 },
    (_, i) => Number('0x' + hex.substr(i * 2, 2)),
  );
}

async function writeAll(w, arr) {
  let nwritten = 0;
  while (nwritten < arr.length) {
    nwritten += await w.write(arr.subarray(nwritten));
  }
}

// https://stackoverflow.com/a/60467595
function md5(inputString) {
  var hc="0123456789abcdef";
  function rh(n) {var j,s="";for(j=0;j<=3;j++) s+=hc.charAt((n>>(j*8+4))&0x0F)+hc.charAt((n>>(j*8))&0x0F);return s;}
  function ad(x,y) {var l=(x&0xFFFF)+(y&0xFFFF);var m=(x>>16)+(y>>16)+(l>>16);return (m<<16)|(l&0xFFFF);}
  function rl(n,c)            {return (n<<c)|(n>>>(32-c));}
  function cm(q,a,b,x,s,t)    {return ad(rl(ad(ad(a,q),ad(x,t)),s),b);}
  function ff(a,b,c,d,x,s,t)  {return cm((b&c)|((~b)&d),a,b,x,s,t);}
  function gg(a,b,c,d,x,s,t)  {return cm((b&d)|(c&(~d)),a,b,x,s,t);}
  function hh(a,b,c,d,x,s,t)  {return cm(b^c^d,a,b,x,s,t);}
  function ii(a,b,c,d,x,s,t)  {return cm(c^(b|(~d)),a,b,x,s,t);}
  function sb(x) {
      var i;var nblk=((x.length+8)>>6)+1;var blks=new Array(nblk*16);for(i=0;i<nblk*16;i++) blks[i]=0;
      for(i=0;i<x.length;i++) blks[i>>2]|=x.charCodeAt(i)<<((i%4)*8);
      blks[i>>2]|=0x80<<((i%4)*8);blks[nblk*16-2]=x.length*8;return blks;
  }
  var i,x=sb(inputString),a=1732584193,b=-271733879,c=-1732584194,d=271733878,olda,oldb,oldc,oldd;
  for(i=0;i<x.length;i+=16) {olda=a;oldb=b;oldc=c;oldd=d;
      a=ff(a,b,c,d,x[i+ 0], 7, -680876936);d=ff(d,a,b,c,x[i+ 1],12, -389564586);c=ff(c,d,a,b,x[i+ 2],17,  606105819);
      b=ff(b,c,d,a,x[i+ 3],22,-1044525330);a=ff(a,b,c,d,x[i+ 4], 7, -176418897);d=ff(d,a,b,c,x[i+ 5],12, 1200080426);
      c=ff(c,d,a,b,x[i+ 6],17,-1473231341);b=ff(b,c,d,a,x[i+ 7],22,  -45705983);a=ff(a,b,c,d,x[i+ 8], 7, 1770035416);
      d=ff(d,a,b,c,x[i+ 9],12,-1958414417);c=ff(c,d,a,b,x[i+10],17,     -42063);b=ff(b,c,d,a,x[i+11],22,-1990404162);
      a=ff(a,b,c,d,x[i+12], 7, 1804603682);d=ff(d,a,b,c,x[i+13],12,  -40341101);c=ff(c,d,a,b,x[i+14],17,-1502002290);
      b=ff(b,c,d,a,x[i+15],22, 1236535329);a=gg(a,b,c,d,x[i+ 1], 5, -165796510);d=gg(d,a,b,c,x[i+ 6], 9,-1069501632);
      c=gg(c,d,a,b,x[i+11],14,  643717713);b=gg(b,c,d,a,x[i+ 0],20, -373897302);a=gg(a,b,c,d,x[i+ 5], 5, -701558691);
      d=gg(d,a,b,c,x[i+10], 9,   38016083);c=gg(c,d,a,b,x[i+15],14, -660478335);b=gg(b,c,d,a,x[i+ 4],20, -405537848);
      a=gg(a,b,c,d,x[i+ 9], 5,  568446438);d=gg(d,a,b,c,x[i+14], 9,-1019803690);c=gg(c,d,a,b,x[i+ 3],14, -187363961);
      b=gg(b,c,d,a,x[i+ 8],20, 1163531501);a=gg(a,b,c,d,x[i+13], 5,-1444681467);d=gg(d,a,b,c,x[i+ 2], 9,  -51403784);
      c=gg(c,d,a,b,x[i+ 7],14, 1735328473);b=gg(b,c,d,a,x[i+12],20,-1926607734);a=hh(a,b,c,d,x[i+ 5], 4,    -378558);
      d=hh(d,a,b,c,x[i+ 8],11,-2022574463);c=hh(c,d,a,b,x[i+11],16, 1839030562);b=hh(b,c,d,a,x[i+14],23,  -35309556);
      a=hh(a,b,c,d,x[i+ 1], 4,-1530992060);d=hh(d,a,b,c,x[i+ 4],11, 1272893353);c=hh(c,d,a,b,x[i+ 7],16, -155497632);
      b=hh(b,c,d,a,x[i+10],23,-1094730640);a=hh(a,b,c,d,x[i+13], 4,  681279174);d=hh(d,a,b,c,x[i+ 0],11, -358537222);
      c=hh(c,d,a,b,x[i+ 3],16, -722521979);b=hh(b,c,d,a,x[i+ 6],23,   76029189);a=hh(a,b,c,d,x[i+ 9], 4, -640364487);
      d=hh(d,a,b,c,x[i+12],11, -421815835);c=hh(c,d,a,b,x[i+15],16,  530742520);b=hh(b,c,d,a,x[i+ 2],23, -995338651);
      a=ii(a,b,c,d,x[i+ 0], 6, -198630844);d=ii(d,a,b,c,x[i+ 7],10, 1126891415);c=ii(c,d,a,b,x[i+14],15,-1416354905);
      b=ii(b,c,d,a,x[i+ 5],21,  -57434055);a=ii(a,b,c,d,x[i+12], 6, 1700485571);d=ii(d,a,b,c,x[i+ 3],10,-1894986606);
      c=ii(c,d,a,b,x[i+10],15,   -1051523);b=ii(b,c,d,a,x[i+ 1],21,-2054922799);a=ii(a,b,c,d,x[i+ 8], 6, 1873313359);
      d=ii(d,a,b,c,x[i+15],10,  -30611744);c=ii(c,d,a,b,x[i+ 6],15,-1560198380);b=ii(b,c,d,a,x[i+13],21, 1309151649);
      a=ii(a,b,c,d,x[i+ 4], 6, -145523070);d=ii(d,a,b,c,x[i+11],10,-1120210379);c=ii(c,d,a,b,x[i+ 2],15,  718787259);
      b=ii(b,c,d,a,x[i+ 9],21, -343485551);a=ad(a,olda);b=ad(b,oldb);c=ad(c,oldc);d=ad(d,oldd);
  }
  return rh(a)+rh(b)+rh(c)+rh(d);
}
