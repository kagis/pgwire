/**
 * @typedef {{
 *  hostname: string,
 *  port: number,
 *  user: string|Uint8Array,
 *  password: string|Uint8Array,
 *  database: string|Uint8Array,
 *  application_name: string|Uint8Array,
 * }} PGConnectOptions
 * */

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
        _networking.canReconnect(err) ||
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


export function pgliteral(s) {
  if (s == null) {
    return 'NULL';
  }
  // TODO array
  return `'` + String(s).replace(/'/g, `''`) + `'`;
}

export function pgident(...segments) {
  return (
    segments
    .map(it => '"' + it.replace(/"/g, '""') + '"')
    .join('.')
  );
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
        password: decodeURIComponent(uri.password) || undefined,
        'user': decodeURIComponent(uri.username) || undefined,
        'database': decodeURIComponent(uri.pathname).replace(/^[/]/, '') || undefined,
      }),
    );
  }
  return Object.assign(computeConnectionOptions(rest), uriOrObj);

  function withoutUndefinedProps(obj) {
    return JSON.parse(JSON.stringify(obj));
  }
}

export function pgerrcode(err) {
  return err?.[kErrorCode];
}

const kErrorCode = Symbol('kErrorCode');

class PgError extends Error {
  constructor({ message, ...props }) {
    const code = props.code;
    super(`[PGERR_${code}] ${message}`);
    this.name = 'PgError';
    this[kErrorCode] = code;
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
    // and this is more difficult to implement than client side timeout
    this._poolIdleTimeout = Math.max(poolIdleTimeout, 0);
  }
  query(...args) {
    return new Response(this._query(args));
  }
  async * _query(args) {
    const conn = this._getConnection();
    try {
      yield * conn.query(...args);
      // const response = activateIterator(conn.query(...args));
      // const discard = Promise.resolve(conn.query(`discard all`));
      // discard.catch(Boolean);
      // try {
      //   yield * response;
      // } finally {
      //   await discard;
      // }
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
    // return destroyReason;
    // TODO should keep and throw destroyReason when querying ?
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
      // discard all
      // - people say that it is slow as creating new connection.
      // + it will cause error when connection is idle in transaction

      // rollback
      // - generates noisy notices in server log

      // begin; rollback;
      // + no notices when no active transactions.
      // + 'begin' causes notice warning when in transaction.
      // - will be not executed as single query when connection is errored in transaction

      // TODO check if query pipelining actually improves perfomance

      // Next query can be already executed and can modify db
      // so destroying connection cannot prevent next query execution.
      // But next query will be executed in explicit transaction
      // so modifications of next query will not be commited automaticaly.
      // Rather then next query does explicit COMMIT
      conn.destroy(Error('pooled connection left in transaction'));
      throw Error('this query lefts pooled connection in transaction');
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
  /** exports pgwire.pgerrcode for cases when Pool is injected as dependency
   * and we dont want to import pgwire directly for error handling code */
  errcode(...args) {
    return pgerrcode(...args);
  }
}


// function activateIterator(iterable) {
//   const iterator = iterable[Symbol.asyncIterator]();
//   const first = iterator.next();
//   first.catch(Boolean);
//   return wrapper();

//   async function * wrapper() {
//     try {
//       const { done, value } = await first;
//       if (done) return;
//       yield value;
//       yield * iterator;
//     } finally {
//       await iterator.return();
//     }
//   }
// }

class Connection {
  constructor({ hostname, port, password, '.debug': debug, ...startupOptions }) {
    this._connectOptions = { hostname, port };
    this._user = startupOptions['user'];
    this._password = password;
    this._debug = ['true', 'on', '1', 1, true].includes(debug);
    this._socket = null;
    this._backendKeyData = null;
    this._parameters = Object.create(null); // TODO retry init
    this._lastErrorResponse = null;
    this._responseRxs = /** @type {Channel[]} */ [];
    this._notificationSubscriptions = /** @type {Set<Channel>} */ new Set();
    this._transactionStatus = null;
    this._fields = null;
    this._clientTextDecoder = new TextDecoder('utf-8', { fatal: true });
    this._copyingOut = false;
    this._copyBuf = new Uint8Array(1024);
    this._copyBufPos = 0;
    this._rowsChunk = [];
    this._tx = new Channel();
    this._txReadable = this._tx; // _tx can be nulled after _tx.end(), but _txReadable will be available
    this._startTx = new Channel(); // TODO retry init
    this._resolveReady = null;
    this._rejectReady = null;
    this._whenReady = new Promise(this._whenReadyExecutor.bind(this));
    this._whenReady.catch(warnError);
    this._saslScramSha256 = null;
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
    ) && this._transactionStatus; // avoid T|E value erasure
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
      if (errorResponse) {
        // TODO also save callstack for network errors
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
      _networking.close(this._socket); // TODO can throw
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
      // so new queries will be rejected with the same error before
      // and after ready resolved/rejected
      this._destroyReason = destroyReason;
    }
    if (this._notificationSubscriptions) {
      this._notificationSubscriptions.forEach(it => void it.end(destroyReason));
      this._notificationSubscriptions = null;
    }
    // TODO await _flushDataRows
    const responseEndReason = destroyReason || Error('incomplete response');
    while (this._responseRxs.length) {
      this._responseRxs.shift().end(responseEndReason);
    }
    // TODO do CancelRequest to wake stuck connection which can continue executing ?
    // return destroyReason; // user code can call .destroy and throw destroyReason in one line
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
  /** starts logical replication
   * @param {{
   *  slot: string,
   *  startLsn: string,
   *  options: object,
   *  ackIntervalMillis: number,
   * }}
   * @returns {AsyncIterable}
   */
  logicalReplication({ slot, startLsn = '0/0', options = {}, ackIntervalMillis = 10e3 }) {
    const optionsSql = (
      Object.entries(options)
      // TODO fix option key is injectable
      .map(([k, v]) => k + ' ' + pgliteral(v))
      .join(',')
      .replace(/.+/, '($&)')
    );
    // TODO get wal_sender_timeout
    const startReplSql = `START_REPLICATION SLOT ${pgident(slot)} LOGICAL ${startLsn} ${optionsSql}`;
    const tx = new Channel();
    const q = this.query(startReplSql, { stdins: [tx] });
    const rx = q[Symbol.asyncIterator]();
    const stream = new ReplicationStream(rx, tx, ackIntervalMillis);
    stream.ack(startLsn); // set initial lsn and also validate lsn
    return stream;

    // TODO how to wait for CopyBothResponse ?
  }
  async _cancelRequest() {
    // await this._whenReady; // wait for backendkey
    if (!this._backendKeyData) {
      throw Error('trying to cancel before BackendKeyData received');
    }
    const socket = await this._createSocket();
    try {
      await _networking.write(socket, serializeFrontendMessage(
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
        await _networking.write(socket, serializeFrontendMessage(new SSLRequest()));
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
      this._startTx.push(new StartupMessage({
        ...startupOptions,
        'client_encoding': 'UTF8', // TextEncoder supports UTF8 only, so hardcode and force UTF8
        // client_encoding: 'win1251',
      }));
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
    } catch (err) {
      caughtError = err;
    }
    this.destroy(caughtError);
  }
  async _sendMessage(m) {
    if (m[Symbol.asyncIterator]) { // pipe messages
      const iter = m[Symbol.asyncIterator]();
      try {
        for (let value, done = false; !done; ) {
          [{ value, done }] = await Promise.all([
            iter.next(),
            value && this._sendMessage(value),
          ]);
        }
      } finally {
        // TODO iter.return resolved on recvMessages side,
        // but recvMessages will not accept messages if sendMessage
        // failed with error before emit frontent message,
        // which causes deadlock here
        // await iter.return();
        // need a way to abort iter.next() here
        // or get rid of parallel piping

        iter.return().catch(warnError); // TODO is this ok to not await ?
        // there are limmited types of iterators passed in _sendMessage
        // - startTx Channel, queryTx Channel and copyWrap iterator
        // no one of them will throw error
      }
    } else {
      if (this._debug) {
        console.log(... m.payload === undefined
          ? ['<- %c%s%c', 'font-weight: bold; color: magenta', m.tag, '']
          : ['<- %c%s%c %o', 'font-weight: bold; color: magenta', m.tag, '', m.payload],
        );
      }
      // TODO serializeFrontendMessage creates new Uint8Array
      // per every call. Should reuse buffer.
      // And should send all messages of query in single writeAll call
      // (except copy from stdin)
      // TODO zero copy for stdin
      await _networking.write(this._socket, serializeFrontendMessage(m));
    }
  }
  async _recvMessages() {
    for await (const mchunk of iterBackendMessages(this._socket)) {
      // TODO we can ? alloc _copyBuf here with size of undelying chunk buffer
      // so we can avoid buffer grow algorithm.
      // Also we can use copyBuf to store binary "decoded" values
      // instead of alloc Uint8Array for each binary value.
      // May be its better to alloc new Uint8Array for each chunk in iterBackendMessages
      // when do many COPY TO STDOUT (will not get countinious chunk) or selecting byteas
      for (const m of mchunk) {
        if (this._debug) {
          console.log(... m.payload === undefined
            ? ['-> %s', m.tag]
            : ['-> %s %o', m.tag, m.payload],
          );
        }
        // TODO check if connection is destroyed to prevent errors in _recvMessage?
        const maybePromise = this._recvMessage(m);
        if (maybePromise) {
          await maybePromise;
        }
      }
      await this._flushDataRows();
    }
    if (this._lastErrorResponse) {
      throw new PgError(this._lastErrorResponse);
    }
    // TODO handle unexpected connection close when sendMessage still working.
    // if sendMessage(this._startTx) is not resolved yet
    // then _startup will not be resolved. But there no more active socket
    // to keep process running
  }
  _recvMessage(m) {
    switch (m.tag) {
      case 'DataRow': return this._recvDataRow(m, m.payload);
      case 'CopyData': return this._recvCopyData(m, m.payload);
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
      case 'AuthenticationSASLFinal': return this._recvAuthenticationSASLFinal(m, m.payload);
      case 'AuthenticationOk': return this._recvAuthenticationOk(m, m.payload);
      case 'ParameterStatus': return this._recvParameterStatus(m, m.payload);
      case 'BackendKeyData': return this._recvBackendKeyData(m, m.payload);
      case 'NoticeResponse': return this._recvNoticeResponse(m, m.payload);
      case 'ErrorResponse': return this._recvErrorResponse(m, m.payload);
      case 'ReadyForQuery': return this._recvReadyForQuery(m, m.payload);

      case 'NotificationResponse': return this._recvNotificationResponse(m.payload, m);
    }
  }
  _recvDataRow(_, /** @type {Array<Uint8Array>} */ row) {
    for (let i = 0; i < this._fields.length; i++) {
      const valbuf = row[i];
      if (valbuf == null) continue;
      const { binary, typeid } = this._fields[i];
      // TODO avoid this._clientTextDecoder.decode for bytea
      row[i] = (
        binary
          // do not valbuf.slice() because nodejs Buffer .slice does not copy
          // TOFO but we not going to reveice Buffer here ?
          ? Uint8Array.prototype.slice.call(valbuf)
          : typeDecode(this._clientTextDecoder.decode(valbuf), typeid)
      );
    }
    this._rowsChunk.push(row);
  }
  _recvCopyData(_, /** @type {Uint8Array} */ copyData) {
    if (!this._copyingOut) {
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
      // CopyData-keepalive message can be received here after CopyDone
      // but before ReadyForQuery
      //
      // Query('CREATE_REPLICATION_SLOT b LOGICAL test_decoding')
      // sometimes CopyData-keepalive message can be received here
      // before RowDescription message
      return;
    }

    if (this._copyBuf.length < this._copyBufPos + copyData.length) { // grow _copyBuf
      const oldbuf = this._copyBuf;
      this._copyBuf = new Uint8Array(oldbuf.length * 2);
      this._copyBuf.set(oldbuf);
    }
    this._copyBuf.set(copyData, this._copyBufPos);
    this._rowsChunk.push(this._copyBuf.subarray(
      this._copyBufPos,
      this._copyBufPos += copyData.length,
    ));
  }
  async _flushDataRows() {
    if (!this._rowsChunk.length) {
      return;
    }
    const m = this._copyBuf.subarray(0, this._copyBufPos);
    m.rows = this._rowsChunk;
    m.tag = null; // TODO m.message ? consistent with .query({ message: 'Parse' }, ...)
    m.payload = undefined;
    const [responseRx] = this._responseRxs;
    await responseRx.push(m);
    this._rowsChunk = [];
    this._copyBufPos = 0;
    // TODO bench reuse single _copyBuf vs new _copyBuf for each chunk
    // Be carefull when commenting next line because awaiting responseRx.push
    // does not awaits until _copyBuf is consumed and ready to resuse
    this._copyBuf = new Uint8Array(this._copyBuf.length);
  }
  async _recvCopyInResponse(m) {
    await this._fwdBackendMessage(m);
  }
  async _recvCopyOutResponse(m) {
    this._copyingOut = true;
    await this._fwdBackendMessage(m);
  }
  async _recvCopyBothResponse(m) {
    this._copyingOut = true;
    await this._fwdBackendMessage(m);
  }
  async _recvCopyDone(m) {
    this._copyingOut = false;
    await this._fwdBackendMessage(m);
  }
  async _recvCommandComplete(m) {
    // when call START_REPLICATION second time then replication is not started,
    // but CommandComplete received right after CopyBothResponse without CopyDone.
    // I cannot find any documentation about this postgres behavior.
    // Seems that this line is responsible for this
    // https://github.com/postgres/postgres/blob/0266e98c6b865246c3031bbf55cb15f330134e30/src/backend/replication/walsender.c#L2307
    // streamingDoneReceiving and streamingDoneSending not reset to false before replication start
    this._copyingOut = false;
    await this._fwdBackendMessage(m);
  }
  async _recvEmptyQueryResponse(m) {
    await this._fwdBackendMessage(m);
  }
  async _recvPortalSuspended(m) {
    await this._fwdBackendMessage(m);
  }
  async _recvNoData(m) {
    await this._fwdBackendMessage(m);
  }
  async _recvRowDescription(m, fields) {
    this._fields = fields;
    await this._fwdBackendMessage(m);
  }
  _recvAuthenticationCleartextPassword() {
    if (this._password == null) {
      throw Error('password required (clear)');
    }
    // should be always encoded as utf8 even when server_encoding is win1251
    this._startTx.push(new PasswordMessage(this._password));
  }
  _recvAuthenticationMD5Password(_, { salt }) {
    if (this._password == null) {
      throw Error('password required (md5)');
    }
    // should use server_encoding, but there is
    // no way to know server_encoding before authentication.
    // So it should be possible to provide password as byte-string
    const utf8enc = new TextEncoder();
    const passwordb = this._password instanceof Uint8Array ? this._password : utf8enc.encode(this._password);
    const userb = this._user instanceof Uint8Array ? this._user : utf8enc.encode(this._user);
    const a = utf8enc.encode(hexEncode(md5(Uint8Array.of(...passwordb, ...userb))));
    const b = 'md5' + hexEncode(md5(Uint8Array.of(...a, ...salt)));
    this._startTx.push(new PasswordMessage(b));
    function hexEncode(/** @type {Uint8Array} */ bytes) {
      return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
    }
  }
  async _recvAuthenticationSASL(_, { mechanism }) {
    if (mechanism != 'SCRAM-SHA-256') {
      // TODO gracefull terminate (send Terminate before socket close) ?
      throw Error(`unsupported SASL mechanism ${mechanism}`);
    }
    this._saslScramSha256 = new SaslScramSha256();
    const firstmsg = await this._saslScramSha256.start();
    const utf8enc = new TextEncoder();
    this._startTx.push(new SASLInitialResponse({
      mechanism: 'SCRAM-SHA-256',
      data: utf8enc.encode(firstmsg),
    }));
  }
  async _recvAuthenticationSASLContinue(_, data) {
    if (this._password == null) {
      throw Error('password required (scram-sha-256)');
    }
    const utf8enc = new TextEncoder();
    const utf8dec = new TextDecoder();
    const finmsg = await this._saslScramSha256.continue(
      utf8dec.decode(data),
      this._password,
    );
    this._startTx.push(new SASLResponse(utf8enc.encode(finmsg)));
  }
  _recvAuthenticationSASLFinal(_, data) {
    const utf8dec = new TextDecoder();
    this._saslScramSha256.finish(utf8dec.decode(data));
    this._saslScramSha256 = null;
  }
  _recvAuthenticationOk() {
    // we dont need password anymore, its more secure to forget it
    this._password = null;
    // TODO we can receive ErrorResponse after AuthenticationOk
    // and if we going to implement connectRetry in Connection
    // then we still need _password here
  }

  _recvErrorResponse(_, payload) {
    this._lastErrorResponse = payload;
    this._copyingOut = false;

    // TODO ErrorResponse is associated with query only when followed by ReadyForQuery
    // ErrorResonse can be received when socket closed by server, and .query can be
    // called just before socket closed
    // if (!this._fwdBackendMessage(m)) {
    //   // TODO wait until socket close and check for _lastErrorResponse
    //   // throw new PgError(m.payload);
    // }
  }
  async _recvReadyForQuery(m, { transactionStatus }) {
    this._transactionStatus = transactionStatus;
    if (this._startTx) { // complete startup
      this._startTx.push(this._txReadable);
      this._startTx.end();
      this._startTx = null;
      this._resolveReady();
      return;
    }
    await this._fwdBackendMessage(m)
    this._responseRxs.shift().end(null, this._lastErrorResponse);
    this._lastErrorResponse = null;
  }

  async _recvNotificationResponse(_, payload) {
    await Promise.all(Array.from(
      this._notificationSubscriptions,
      nc => nc.push(payload)
    ));
  }
  _recvNoticeResponse(m) {
    // TODO async NoticeResponse can be recevied after .query called
    // but before query messages actually received by postres.
    // Such NoticeResponse will be uncorrectly forwared to query response.
    // Seems that there is no way to check whether NoticeResponse
    // belongs to query or not. May be we should treat all NoticeResponse
    // messages as connection level notices and do not forward NoticeResponses
    // to query responses at all.

    // if (!this._fwdBackendMessage(m)) {
    //   // NoticeResponse is not associated with query
    //   // TODO report nonquery NoticeResponse
    // }
  }
  _recvParameterStatus(_, { parameter, value }) {
    this._parameters[parameter] = value;
    // TODO emit event ?
  }
  _recvBackendKeyData(_, backendKeyData) {
    this._backendKeyData = backendKeyData;
  }
  async _fwdBackendMessage({ tag, payload }) {
    await this._flushDataRows();
    const m = new Uint8Array(0);
    m.rows = [];
    m.tag = tag;
    m.payload = payload;
    const [responseRx] = this._responseRxs;
    await responseRx.push(m);
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

function warnError(err) {
  // console.trace('warning', err);
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
function * extendedQueryStatement({ statement, params = [], limit, binary, stdin, noBuffer }, stdinAbortSignal) {
  const paramTypes = params.map(({ type }) => type);
  const flush = []; // [new Flush()];
  yield * extendedQueryParse({ statement, paramTypes });
  yield * flush;
  yield * extendedQueryBind({ params, binary });
  yield * flush;
  yield * extendedQueryExecute({ limit, stdin }, stdinAbortSignal);
  yield * flush;
}
function * extendedQueryParse({ statement, statementName, paramTypes = [] }) {
  paramTypes = paramTypes.map(typeResolve);
  yield new Parse({ statement, statementName, paramTypes });
}
function * extendedQueryBind({ portal, statementName, binary, params = [] }) {
  params = params.map(({ value, type }) => typeEncode(value, typeResolve(type)));
  yield new Bind({ portal, statementName, binary, params });
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
    // it can be handy when we need to enqueue .end after .query immediatly
    // or if need to enqueue .query('discard all') after user .query in pool
    return this._iter[Symbol.asyncIterator]();
  }
  then(...args) {
    return this._loadOnce().then(...args);
  }
  catch(...args) {
    return this._loadOnce().catch(...args);
  }
  finally(...args) {
    return this._loadOnce().finally(...args);
  }
  async _loadOnce() {
    return this._loadPromise || (this._loadPromise = this._load());
  }
  async _load() {
    let inTransaction = false;
    let lastResult;
    const results = [];
    for await (const chunk of this) {
      lastResult = lastResult || {
        rows: [],
        // TODO copied: [],
        // notices: [],
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
        case 'ReadyForQuery':
          inTransaction = chunk.payload.transactionStatus != 0x49 /*I*/;
          continue;
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
      inTransaction,
      results,
      // TODO root `rows` concat ?
      // get notices() {
      //   return results.flatMap(({ notices }) => notices);
      // },
    };
  }
}

class ReplicationStream {
  constructor(rx, tx, ackIntervalMillis) {
    this._rx = rx;
    this._tx = tx;
    this._ackIntervalMillis = ackIntervalMillis;
    this._ackingLsn = '00000000/00000000';
    this._iter = this._iterate(); // TODO can use `this` before all props inited? (hidden class optimization)
  }
  [Symbol.asyncIterator]() {
    return this._iter[Symbol.asyncIterator]();
  }
  async * pgoutput() {
    const relationCache = Object.create(null);
    const typeCache = Object.create(null);
    for await (const chunk of this) {
      yield chunk.map(decodePgoutputMessage);
    }
    function decodePgoutputMessage(xlogData) {
      const pgoreader = new PgoutputMessageReader(xlogData, relationCache, typeCache);
      return pgoreader.readPgoutputMessage()
    }
  }
  async * _iterate() {
    // TODO start timer after successfull replication start.
    // CopyBothResponse is not best option because replication
    // can be aborted immediatly after CopyBothResponse when
    // replication is started second time on connection.
    // So after first PrimaryKeepaliveMessage?
    const ackTimer = setInterval(this._ackImmediate.bind(this), this._ackIntervalMillis);
    try {
      for (let chunk, done;;) {
        ({ value: chunk, done } = await this._rx.next());
        if (done) break;
        const xlogMessages = [];
        for (const copyData of chunk.rows) {
          const replmsgReader = new ReplicationMessageReader(copyData);
          const replmsg = replmsgReader.readReplicationMessage();
          if (replmsg.tag == 'XLogData') {
            xlogMessages.push(replmsg);
            continue;
          }
          if (replmsg.tag == 'PrimaryKeepaliveMessage' && replmsg.shouldReply) {
            this._ackImmediate();
          }
        }
        // TODO we can emit empty replication message (fwd PrimaryKeepAlive?) every 1sec
        // so user have a chance to `break` loop
        yield xlogMessages;
      }
    } finally {
      clearInterval(ackTimer);
      this._ackImmediate();
      this._tx.end();
      // TODO handle errors?
      for await (const _ of this._rx); // drain rx until end
    }
  }
  ack(lsn) {
    if (!/^[0-9a-f]{1,8}[/][0-9a-f]{1,8}$/i.test(lsn)) {
      throw TypeError('invalid lsn');
    }
    lsn = lsnMakeComparable(lsn);
    if (lsn > this._ackingLsn) {
      this._ackingLsn = lsn;
    }
  }
  _ackImmediate() {
    // if (!this._tx) return;
    const msg = new Uint8Array(1 + 8 + 8 + 8 + 8 + 1);
    const msgv = new DataView(msg.buffer);
    let nlsn = BigInt('0x' + this._ackingLsn.replace('/', ''));
    nlsn += 1n;
    msgv.setUint8(0, 0x72); // r
    msgv.setBigUint64(1, nlsn);
    msgv.setBigUint64(9, nlsn);
    msgv.setBigUint64(17, nlsn);
    // TODO push the same buf every time, update msg in .ack
    this._tx.push(msg); // TODO backpreassure
  }
}

async function * iterBackendMessages(socket) {
  let buf = new Uint8Array(16_640);
  let nbuf = 0;
  for (;;) {
    if (nbuf >= buf.length) { // grow buffer
      const oldbuf = buf;
      buf = new Uint8Array(oldbuf.length * 2); // TODO prevent uncontrolled grow
      buf.set(oldbuf);
    }
    const nread = await _networking.read(socket, buf.subarray(nbuf));
    if (nread == null) break;
    nbuf += nread;

    let nparsed = 0;
    const messages = [];
    for (;;) {
      const itag = nparsed;
      const isize = itag + 1;
      const ipayload = isize + 4;
      if (nbuf < ipayload) break; // incomplete message
      const size = buf[isize] << 24 | buf[isize + 1] << 16 | buf[isize + 2] << 8 | buf[isize + 3];
      if (size < 4) {
        throw Error('invalid backend message size');
      }
      const inext = isize + size;
      if (nbuf < inext) break; // incomplete message
      const msgreader = new BackendMessageReader(buf.subarray(ipayload, inext))
      const message = msgreader.readBackendMessage(buf[itag]);
      messages.push(message);
      // TODO batch DataRow here
      nparsed = inext;
    }
    yield messages;

    if (nparsed) { // TODO check if copyWithin(0, 0) is noop
      buf.copyWithin(0, nparsed, nbuf); // move unconsumed bytes to begining of buffer
      nbuf -= nparsed;
    }
  }
}

function serializeFrontendMessage(message) {
  // FIXME reuse buffer
  const bytes = new Uint8Array(message.size);
  message.writeTo(new MessageWriter(bytes.buffer));
  return bytes;
}

class FrontendMessage {
  constructor(payload) {
    this.payload = payload;
    this.size = 0;
    const sizeCounter = new MessageSizeCounter();
    this._write(sizeCounter, null, payload);
    this.size = sizeCounter.result;
  }
  get tag() {
    return this.constructor.name; // we will use it only for debug logging
  }
  writeTo(messageWriter) {
    this._write(messageWriter, this.size, this.payload);
  }
}

class StartupMessage extends FrontendMessage {
  _write(w, size, options) {
    w.writeInt32BE(size);
    w.writeInt32BE(0x00030000);
    for (const [key, val] of Object.entries(options)) {
      w.writeString(key);
      w.writeString(val);
    }
    w.writeUint8(0);
  }
}

class CancelRequest extends FrontendMessage {
  _write(w, _size, { pid, secretKey }) {
    w.writeInt32BE(16);
    w.writeInt32BE(80877102); // (1234 << 16) | 5678
    w.writeInt32BE(pid);
    w.writeInt32BE(secretKey);
  }
}

class SSLRequest extends FrontendMessage {
  _write(w) {
    w.writeInt32BE(8);
    w.writeInt32BE(80877102); // (1234 << 16) | 5678
  }
}

class PasswordMessage extends FrontendMessage {
  _write(w, size, payload) {
    w.writeUint8(0x70); // p
    w.writeInt32BE(size - 1);
    w.writeString(payload);
  }
}

class SASLInitialResponse extends FrontendMessage {
  _write(w, size, { mechanism, data }) {
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
  _write(w, size, data) {
    w.writeUint8(0x70); // p
    w.writeInt32BE(size - 1);
    w.write(data);
  }
}

class Query extends FrontendMessage {
  _write(w, size) {
    w.writeUint8(0x51); // Q
    w.writeInt32BE(size - 1);
    w.writeString(this.payload);
  }
}

class Parse extends FrontendMessage {
  _write(w, size, { statement, statementName = '', paramTypes = [] }) {
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
  _write(w, size, { portal = '', statementName = '', params = [], binary = [] }) {
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
      // TODO avoid twice encoding
      if (!(p instanceof Uint8Array)) {
        encoded = utf8encoder.encode(p);
      }
      w.writeInt32BE(encoded.length);
      // TODO zero copy param encode
      w.write(encoded);
    }
    w.writeInt16BE(binary.length);
    for (const fmt of binary) {
      w.writeInt16BE(fmt);
    }
  }
}

class Execute extends FrontendMessage {
  _write(w, size, { portal = '', limit = 0 }) {
    w.writeUint8(0x45); // E
    w.writeInt32BE(size - 1);
    w.writeString(portal);
    w.writeUint32BE(limit);
  }
}

class DescribeStatement extends FrontendMessage {
  _write(w, size, statementName = '') {
    w.writeUint8(0x44); // D
    w.writeInt32BE(size - 1);
    w.writeUint8(0x53); // S
    w.writeString(statementName);
  }
}

class DescribePortal extends FrontendMessage {
  _write(w, size, portal = '') {
    w.writeUint8(0x44); // D
    w.writeInt32BE(size - 1);
    w.writeUint8(0x50); // P
    w.writeString(portal);
  }
}

class ClosePortal extends FrontendMessage {
  _write(w, size, portal = '') {
    w.writeUint8(2); // C
    w.writeInt32BE(size - 1);
    w.writeUint8(0x50); // P
    w.writeString(portal);
  }
}

class CloseStatement extends FrontendMessage {
  _write(w, size, statementName = '') {
    w.writeUint8(2); // C
    w.writeInt32BE(size - 1);
    w.writeUint8(0x53); // S
    w.writeString(statementName);
  }
}

class Sync extends FrontendMessage {
  _write(w) {
    w.writeUint8(0x53); // S
    w.writeInt32BE(4);
  }
}

// unused
class Flush extends FrontendMessage {
  _write(w) {
    w.writeUint8(0x48); // H
    w.writeInt32BE(4);
  }
}

class CopyData extends FrontendMessage {
  _write(w, size, data) {
    w.writeUint8(0x64); // d
    w.writeInt32BE(size - 1);
    w.write(data);
  }
}

class CopyDone extends FrontendMessage {
  _write(w) {
    w.writeUint8(0x63); // c
    w.writeInt32BE(4);
  }
}

class CopyFail extends FrontendMessage {
  _write(w, size, cause) {
    w.writeUint8(0x66); // f
    w.writeInt32BE(size - 1);
    w.writeString(cause);
  }
}

class Terminate extends FrontendMessage {
  _write(w) {
    w.writeUint8(0x58); // X
    w.writeInt32BE(4);
  }
}

// https://www.postgresql.org/docs/14/protocol-message-types.html
class MessageReader {
  constructor (b) {
    this._b = b;
    this._p = 0;
    this._textDecoder = MessageReader.defaultTextDecoder;
  }
  readUint8() {
    this._checkSize(1);
    return this._b[this._p++];
  }
  readInt16() {
    this._checkSize(2);
    return this._b[this._p++] << 8 | this._b[this._p++];
  }
  readInt32() {
    this._checkSize(4);
    return this._b[this._p++] << 24 | this._b[this._p++] << 16 | this._b[this._p++] << 8 | this._b[this._p++];
  }
  readString() {
    const endIdx = this._b.indexOf(0x00, this._p);
    if (endIdx < 0) {
      throw Error('unexpected end of message');
    }
    const strbuf = this._b.subarray(this._p, endIdx);
    this._p = endIdx + 1;
    return this._textDecoder.decode(strbuf);
  }
  read(n) {
    this._checkSize(n);
    return this._b.subarray(this._p, this._p += n);
  }
  readToEnd() {
    const p = this._p;
    this._p = this._b.length;
    return this._b.subarray(p);
  }
  _checkSize(n) {
    if (this._b.length < this._p + n) {
      throw Error('unexpected end of message');
    }
  }
  // should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
  static defaultTextDecoder = new TextDecoder();
}

// https://www.postgresql.org/docs/14/protocol-message-formats.html
class BackendMessageReader extends MessageReader {
  readBackendMessage(asciiTag) {
    switch (asciiTag) {
      case 0x64 /* d */: return { tag: 'CopyData', payload: this._b };
      case 0x44 /* D */: return { tag: 'DataRow', payload: this.readDataRow() };
      case 0x76 /* v */: return { tag: 'NegotiateProtocolVersion', payload: this.readNegotiateProtocolVersion() };
      case 0x53 /* S */: return { tag: 'ParameterStatus', payload: this.readParameterStatus() };
      case 0x4b /* K */: return { tag: 'BackendKeyData', payload: this.readBackendKeyData() };
      case 0x5a /* Z */: return { tag: 'ReadyForQuery', payload: this.readReadyForQuery() };
      case 0x48 /* H */: return { tag: 'CopyOutResponse', payload: this.readCopyResponse() };
      case 0x47 /* G */: return { tag: 'CopyInResponse', payload: this.readCopyResponse() };
      case 0x57 /* W */: return { tag: 'CopyBothResponse', payload: this.readCopyResponse() };
      case 0x63 /* c */: return { tag: 'CopyDone' };
      case 0x54 /* T */: return { tag: 'RowDescription', payload: this.readRowDescription() };
      case 0x74 /* t */: return { tag: 'ParameterDescription', payload: this.readParameterDescription() };
      case 0x43 /* C */: return { tag: 'CommandComplete', payload: this.readString() };
      case 0x31 /* 1 */: return { tag: 'ParseComplete' };
      case 0x32 /* 2 */: return { tag: 'BindComplete' };
      case 0x33 /* 3 */: return { tag: 'CloseComplete' };
      case 0x73 /* s */: return { tag: 'PortalSuspended' };
      case 0x49 /* I */: return { tag: 'EmptyQueryResponse' };
      case 0x4e /* N */: return { tag: 'NoticeResponse', payload: this.readErrorOrNotice() };
      case 0x45 /* E */: return { tag: 'ErrorResponse', payload: this.readErrorOrNotice() };
      case 0x6e /* n */: return { tag: 'NoData' };
      case 0x41 /* A */: return { tag: 'NotificationResponse', payload: this.readNotificationResponse() };
      case 0x52 /* R */:
        switch (this.readInt32()) {
          case 0x0: return { tag: 'AuthenticationOk' };
          case 0x2: return { tag: 'AuthenticationKerberosV5' };
          case 0x3: return { tag: 'AuthenticationCleartextPassword' };
          case 0x5: return { tag: 'AuthenticationMD5Password', payload: { salt: this.read(4) } };
          case 0x6: return { tag: 'AuthenticationSCMCredential' };
          case 0x7: return { tag: 'AuthenticationGSS' };
          case 0x8: return { tag: 'AuthenticationGSSContinue', payload: this.readToEnd() };
          case 0x9: return { tag: 'AuthenticationSSPI' };
          case 0xa: return { tag: 'AuthenticationSASL', payload: { mechanism: this.readString() } };
          case 0xb: return { tag: 'AuthenticationSASLContinue', payload: this.readToEnd() };
          case 0xc: return { tag: 'AuthenticationSASLFinal', payload: this.readToEnd() };
          default: throw Error('unknown auth message');
        }
      default: return { tag: asciiTag, payload: this._b }; // TODO warn unknown message
    }
  }
  readDataRow() {
    const nfields = this.readInt16()
    const row = Array(nfields);
    for (let i = 0; i < nfields; i++) {
      const valsize = this.readInt32();
      row[i] = valsize < 0 ? null : this.read(valsize);
    }
    return row;
  }
  readNegotiateProtocolVersion() {
    return {
      version: this.readInt32(),
      unrecognizedOptions: Array.from(
        { length: this.readInt32() },
        this.readString,
        this,
      ),
    };
  }
  readParameterDescription() {
    return Array.from(
      { length: this.readInt16() },
      this.readInt32,
      this,
    );
  }
  readBackendKeyData() {
    return {
      pid: this.readInt32(),
      secretKey: this.readInt32(),
    };
  }
  readReadyForQuery() {
    return { transactionStatus: this.readUint8() };
  }
  readCopyResponse() {
    return {
      binary: this.readUint8(),
      binaryPerAttr: Array.from(
        { length: this.readInt16() },
        this.readInt16,
        this,
      ),
    };
  }
  readRowDescription() {
    return Array.from({ length: this.readInt16() }, _ => ({
      name: this.readString(),
      tableid: this.readInt32(),
      column: this.readInt16(),
      typeid: this.readInt32(),
      typelen: this.readInt16(),
      typemod: this.readInt32(),
      binary: this.readInt16(),
    }));
  }
  readParameterStatus() {
    return {
      parameter: this.readString(),
      value: this.readString(),
    };
  }
  readErrorOrNotice() {
    const fields = Array(256);
    for (;;) {
      const fieldCode = this.readUint8();
      if (!fieldCode) break;
      fields[fieldCode] = this.readString();
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
  readNotificationResponse() {
    return {
      pid: this.readInt32(),
      channel: this.readString(),
      payload: this.readString(),
    };
  }
}

// https://www.postgresql.org/docs/14/protocol-replication.html#id-1.10.5.9.7.1.5.1.8
class ReplicationMessageReader extends MessageReader {
  readReplicationMessage() {
    const tag = this.readUint8();
    switch (tag) {
      case 0x77 /*w*/: return {
        tag: 'XLogData',
        lsn: this.readLsn(),
        endLsn: this.readLsn(),
        time: this.readTime(),
        data: this.readToEnd(),
      };
      case 0x6b /*k*/: return {
        tag: 'PrimaryKeepaliveMessage',
        endLsn: this.readLsn(),
        time: this.readTime(),
        shouldReply: this.readUint8(),
      };
      default: throw Error('unknown replication message');
    }
  }
  readLsn() {
    return (
      this.readUint32().toString(16).padStart(8, '0') + '/' +
      this.readUint32().toString(16).padStart(8, '0')
    ).toUpperCase();
  }
  readTime() {
    // (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY == 946684800000000
    return this.readUint64() + 946684800000000n;
  }
  readUint64() {
    return BigInt(this.readUint32()) << 32n | BigInt(this.readUint32());
  }
  readUint32() {
    return this.readInt32() >>> 0;
  }
}

// https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html
class PgoutputMessageReader extends ReplicationMessageReader  {
  constructor({ data, lsn, endLsn, time }, relationCache, typeCache) {
    super(data);
    this._relationCache = relationCache;
    this._typeCache = typeCache;
    this._lsn = lsn;
    this._endLsn = endLsn;
    this._time = time;
  }
  readPgoutputMessage() {
    const tag = this.readUint8();
    switch (tag) {
      case 0x42 /*B*/: return this.readBegin();
      case 0x4d /*M*/: return this.readLogicalMessage();
      case 0x43 /*C*/: return this.readCommit();
      case 0x4f /*O*/: return this.readOrigin();
      case 0x52 /*R*/: return this.readRelation();
      case 0x59 /*Y*/: return this.readType();
      case 0x49 /*I*/: return this.readInsert();
      case 0x55 /*U*/: return this.readUpdate();
      case 0x44 /*D*/: return this.readDelete();
      case 0x54 /*T*/: return this.readTruncate();
      default: throw Error('unknown pgoutput message');
    }
  }
  readBegin() {
    return {
      tag: 'begin',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      finalLsn: this.readLsn(),
      commitTime: this.readTime(),
      xid: this.readInt32(),
    };
  }
  readLogicalMessage() {
    return {
      tag: 'message',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      transactional: this.readUint8(),
      _lsn: this.readLsn(),
      prefix: this.readString(),
      content: this.read(this.readInt32()),
    };
  }
  readCommit() {
    return {
      tag: 'commit',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      flags: this.readUint8(),
      commitLsn: this.readLsn(),
      _endLsn: this.readLsn(), // TODO fix dup
      commitTime: this.readTime(),
    };
  }
  readOrigin() {
    return {
      tag: 'origin',
      lsn: this.readLsn(),
      origin: this.readString(),
    };
  }
  readRelation() {
    const rel = {
      tag: 'relation',
      // lsn: this._lsn,
      // endLsn: this._endLsn,
      // time,
      relationid: this.readInt32(),
      schema: this.readString(),
      name: this.readString(),
      replicaIdentity: String.fromCharCode(this.readUint8()),
      attrs: Array.from({ length: this.readInt16() }, _ => ({
        flags: this.readUint8(), // TODO bool accessors
        name: this.readString(),
        typeid: this.readInt32(),
        typemod: this.readInt32(),
        // TODO lookup typeSchema and typeName
      })),
    };
    // mem leak not likely to happen because number of relations is usually small
    this._relationCache[rel.relationid] = rel;
    return rel;
  }
  readType() {
    const type = {
      tag: 'type',
      typeid: this.readInt32(),
      schema: this.readString(),
      name: this.readString(),
    };
    this._typeCache[type.typeid] = type;
    return type;
  }
  readInsert() {
    const relid = this.readInt32();
    const relation = this._relationCache[relid];
    const _kind = this.readUint8();
    const after = this.readTuple(relation);
    return {
      tag: 'insert',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      relation,
      before: null,
      after,
    };
  }
  readUpdate() {
    const relid = this.readInt32();
    const relation = this._relationCache[relid];
    let kind = this.readUint8();
    let before = null;
    if (kind == 0x4b /*K*/ || kind == 0x4f /*O*/) {
      before = this.readTuple(relation);
      kind = this.readUint8();
    }
    const after = this.readTuple(relation);
    return {
      tag: 'update',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      relation: relation,
      before,
      after,
      // TODO keyOnly ?
    };
  }
  readDelete() {
    const relid = this.readInt32();
    const relation = this._relationCache[relid];
    const kind = this.readUint8();
    const before = this.readTuple(relation);
    return {
      tag: 'delete',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      relation,
      before,
      after: null,
      keyOnly: kind == 0x4b /*K*/,
    };
  }
  readTruncate() {
    const nrels = this.readInt32();
    const flags = this.readUint8();
    return {
      tag: 'truncate',
      lsn: this._lsn,
      endLsn: this._endLsn,
      time: this._time,
      cascade: Boolean(flags & 0b1),
      restartSeqs: Boolean(flags & 0b10),
      relations: Array.from(
        { length: nrels },
        _ => this._relationCache[this.readInt32()],
      ),
    };
  }
  readTuple(relation) {
    const nfields = this.readInt16();
    const tuple = Object.create(null);
    for (let i = 0; i < nfields; i++) {
      const { name, typeid } = relation.attrs[i];
      switch (this.readUint8()) {
        case 0x74 /*t*/:
          const valsize = this.readInt32();
          const valbuf = this.read(valsize);
          const valtext = this._textDecoder.decode(valbuf);
          tuple[name] = typeDecode(valtext, typeid);
          break;
        case 0x6e /*n*/:
          tuple[name] = null;
          break;
        default /*u*/:
          tuple[name] = undefined;
      }
    }
    return tuple;
  }
}


// TODO no DataView
class MessageWriter {
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
    if (val instanceof Uint8Array) {
      this.write(val);
    } else {
      const { read, written } = utf8encoder.encodeInto(val, new Uint8Array(this._buf, this._pos));
      if (read < val.length) {
        throw Error('too small buffer');
      }
      this._pos += written;
    }
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

class MessageSizeCounter {
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
    if (val instanceof Uint8Array) {
      if (val.indexOf(0x00) > 0) {
        throw TypeError('zero char is not allowed');
      }
      this.result += val.length + 1;
    } else if (typeof val == 'string') {
      if (val.indexOf('\0') > 0) {
        throw TypeError('zero char is not allowed');
      }
      this.result += utf8length(val) + 1;
    } else {
      throw TypeError('string or Uint8Array expected');
    }
  }
  write(val) {
    if (!(val instanceof Uint8Array)) {
      throw TypeError('Uint8Array expected');
    }
    this.result += val.length;
  }
}

// https://stackoverflow.com/a/25994411
function utf8length(s) {
  let result = 0;
  for (let i = 0; i < s.length; i++) {
    let c = s.charCodeAt(i);
    result += c >> 11 ? 3 : c >> 7 ? 2 : 1;
  }
  return result;
}

///////// begin type codec

function typeResolve(idOrName) {
  if (typeof idOrName == 'number') {
    return idOrName;
  }
  switch (idOrName) { // add line here when register new type
    case 'text'    : return   25; case 'text[]'    : return 1009;
    case 'uuid'    : return 2950; case 'uuid[]'    : return 2951;
    case 'varchar' : return 1043; case 'varchar[]' : return 1015;
    case 'bool'    : return   16; case 'bool[]'    : return 1000;
    case 'bytea'   : return   17; case 'bytea[]'   : return 1001;
    case 'int2'    : return   21; case 'int2[]'    : return 1005;
    case 'int4'    : return   23; case 'int4[]'    : return 1007;
    case 'float4'  : return  700; case 'float4[]'  : return 1021;
    case 'float8'  : return  701; case 'float8[]'  : return 1022;
    case 'int8'    : return   20; case 'int8[]'    : return 1016;
    case 'json'    : return  114; case 'json[]'    : return  199;
    case 'jsonb'   : return 3802; case 'jsonb[]'   : return 3807;
    case 'pg_lsn'  : return 3220; case 'pg_lsn[]'  : return 3221;
  }
  throw Error('unknown builtin type name ' + JSON.stringify(idOrName));
}

// TODO bytea[]
function typeEncode(value, typeid) {
  if (value instanceof Uint8Array) {
    // treat Uint8Array values as already encoded,
    // so user can receive value with unknown type as Uint8Array
    // from extended .query and pass it back as parameter
    return value;
    // it also handles bytea
  }
  switch (typeid) { // add line here when register new type (optional)
    case  114 /* json    */:
    case 3802 /* jsonb   */:
      return JSON.stringify(value);
  }
  let elemTypeid = typeOfElem(typeid);
  if (elemTypeid) {
    // check if pgencode(value[0]) instanceof uint8Array then do encodeArrayBin otherwise do encodeArrayText
    return typeEncodeArray(value, elemTypeid);
  }
  return String(value);
}

function typeDecode(text, typeid) {
  switch (typeid) { // add line here when register new type
    case   25 /* text    */:
    case 2950 /* uuid    */:
    case 1043 /* varchar */: return text;
    case   16 /* bool    */: return text == 't';
    case   17 /* bytea   */: return typeDecodeBytea(text);
    case   21 /* int2    */:
    case   23 /* int4    */:
    case  700 /* float4  */:
    case  701 /* float8  */: return Number(text);
    case   20 /* int8    */: return BigInt(text);
    case  114 /* json    */:
    case 3802 /* jsonb   */: return JSON.parse(text);
    case 3220 /* pg_lsn  */: return lsnMakeComparable(text);
  }
  let elemTypeid = typeOfElem(typeid);
  if (elemTypeid) {
    return typeDecodeArray(text, elemTypeid);
  }
  return text; // unknown type
}

function typeOfElem(arrayTypeid) {
  switch (arrayTypeid) { // add line here when register new type
    case 1009: return   25; // text
    case 1000: return   16; // bool
    case 1001: return   17; // bytea
    case 1005: return   21; // int2
    case 1007: return   23; // int4
    case 1016: return   20; // int8
    case 1021: return  700; // float4
    case 1022: return  701; // float8
    case  199: return  114; // json
    case 3807: return 3802; // jsonb
    case 3221: return 3220; // pg_lsn
    case 2951: return 2950; // uuid
    case 1015: return 1043; // varchar
  }
}

function typeDecodeArray(text, elemTypeid) {
  text = text.replace(/^\[.+=/, ''); // skip dimensions
  const jsonArray = text.replace(/{|}|,|"(?:[^"\\]|\\.)*"|[^,}]+/gy, token => (
    token == '{' ? '[' :
    token == '}' ? ']' :
    token == 'NULL' ? 'null' :
    token == ',' || token[0] == '"' ? token :
    JSON.stringify(token)
  ));
  return JSON.parse(jsonArray, (_, elem) => (
    typeof elem == 'string' ? typeDecode(elem, elemTypeid) : elem
  ));
}

// TODO multi dimension
// TODO array_nulls https://www.postgresql.org/docs/14/runtime-config-compatible.html#id-1.6.7.16.2.2.1.1.3
function typeEncodeArray(arr, elemTypeid) {
  return JSON.stringify(arr, function (_, elem) {
    return this == arr && elem != null ? encodeElem(elem, elemTypeid) : elem;
  }).replace(/^\[(.*)]$/, '{$1}');
}

function typeDecodeBytea(text) {
  // https://www.postgresql.org/docs/9.6/datatype-binary.html#AEN5830
  if (text.startsWith('\\x')) {
    const hex = text.slice(2); // TODO check hex.length is even ?
    const bytes = new Uint8Array(hex.length >> 1);
    for (let i = 0, m = 4; i < hex.length; i++, m ^= 4) {
      let d = hex.charCodeAt(i);
      if (0x30 <= d && d <= 0x39) d -= 0x30; // 0-9
      else if (0x41 <= d && d <= 0x46) d -= 0x41 - 0xa; // A-F
      else if (0x61 <= d && d <= 0x66) d -= 0x61 - 0xa; // a-f
      else throw Error(`invalid hex digit 0x${d}`);
      bytes[i >> 1] |= d << m; // m==4 on even iter, m==0 on odd iter
    }
    return bytes;
  }
  return Uint8Array.from( // legacy escape format TODO no eval
    Function(text.replace('"', '\\"').replace(/.*/, 'return "$&"')).call(),
    x => x.charCodeAt(),
  );
}

function lsnMakeComparable(text) {
  const [h, l] = text.split('/');
  return h.padStart(8, '0') + '/' + l.padStart(8, '0');
}

///////////// end type codec


const utf8encoder = new TextEncoder();


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

class SaslScramSha256 {
  _clientFirstMessageBare;
  _serverSignatureB64;
  async start() {
    const clientNonce = new Uint8Array(24);
    crypto.getRandomValues(clientNonce);
    this._clientFirstMessageBare = 'n=,r=' + this._b64encode(clientNonce);
    return 'n,,' + this._clientFirstMessageBare;
  }
  async continue(serverFirstMessage, password) {
    const utf8enc = new TextEncoder();
    const { 'i': iterations, 's': saltB64, 'r': nonceB64 } = this._parseMsg(serverFirstMessage);
    const finalMessageWithoutProof = 'c=biws,r=' + nonceB64;
    const salt = this._b64decode(saltB64);
    const passwordUtf8 = utf8enc.encode(password.normalize());
    const saltedPassword = await this._pbkdf2(passwordUtf8, salt, +iterations, 32 * 8);
    const clientKey = await this._hmac(saltedPassword, utf8enc.encode('Client Key'));
    const storedKey = await this._sha256(clientKey);
    const authMessage = utf8enc.encode(
      this._clientFirstMessageBare + ',' +
      serverFirstMessage + ',' +
      finalMessageWithoutProof
    );
    const clientSignature = await this._hmac(storedKey, authMessage);
    const clientProof = this._xor(clientKey, clientSignature);
    const serverKey = await this._hmac(saltedPassword, utf8enc.encode('Server Key'));
    this._serverSignatureB64 = this._b64encode(await this._hmac(serverKey, authMessage));
    return finalMessageWithoutProof + ',p=' + this._b64encode(clientProof);
  }
  finish(response) {
    if (!this._serverSignatureB64) {
      throw Error('unexpected auth finish');
    }
    const { 'v': receivedServerSignatureB64 } = this._parseMsg(response);
    if (this._serverSignatureB64 != receivedServerSignatureB64) {
      throw Error('invalid server SCRAM signature');
    }
  }
  _parseMsg(msg) { // parses `key1=val,key2=val` into object
    return Object.fromEntries(msg.split(',').map(it => /^(.*?)=(.*)$/.exec(it).slice(1)));
  }
  async _sha256(val) {
    return new Uint8Array(await crypto.subtle.digest('SHA-256', val));
  }
  async _pbkdf2(pwd, salt, iterations, len) {
    const cryptoKey = await crypto.subtle.importKey('raw', pwd, 'PBKDF2', false, ['deriveBits']);
    const pbkdf2params = { name: 'PBKDF2', hash: 'SHA-256', salt, iterations };
    const buf = await crypto.subtle.deriveBits(pbkdf2params, cryptoKey, len);
    return new Uint8Array(buf);
  }
  async _hmac(key, inp) {
    const hmacParams = { name: 'HMAC', hash: 'SHA-256' };
    const importedKey = await crypto.subtle.importKey('raw', key, hmacParams, false, ['sign']);
    const buf = await crypto.subtle.sign('HMAC', importedKey, inp);
    return new Uint8Array(buf);
  }
  _xor(a, b) {
    return Uint8Array.from(a, (x, i) => x ^ b[i]);
  }
  _b64encode(bytes) {
    return btoa(String.fromCharCode(...bytes));
  }
  _b64decode(b64) {
    return Uint8Array.from(atob(b64), x => x.charCodeAt());
  }
}

const _networking = {
  async connect(options) {
    return Deno.connect(options);
  },
  canReconnect(err) {
    return (
      err instanceof Deno.errors.ConnectionRefused ||
      err instanceof Deno.errors.ConnectionReset
    );
  },
  async startTls(socket) {
    return Deno.startTls(socket);
  },
  async write(socket, arr) {
    let nwritten = 0;
    while (nwritten < arr.length) {
      nwritten += await socket.write(arr.subarray(nwritten));
    }
  },
  async read(socket, buf) {
    return socket.read(buf);
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

// We will not use Deno std to make this module compatible with Node
function md5(/** @type {Uint8Array} */ input) {
  const padded = new Uint8Array(Math.ceil((input.byteLength + 1 + 8) / 64) * 64);
  const paddedv = new DataView(padded.buffer);
  padded.set(input);
  paddedv.setUint8(input.byteLength, 0b10000000);
  paddedv.setBigUint64(paddedv.byteLength - 8, BigInt(input.byteLength) * 8n, true);

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;
  const m = Array(16);

  for (let i = 0; i < paddedv.byteLength; i += 64) {
    let a = a0, b = b0, c = c0, d = d0;
    for (let j = 0; j < 16; j++) {
      m[j] = paddedv.getUint32(i + j * 4, true);
    }

    // round 1
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x0] + 0xd76aa478, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x1] + 0xe8c7b756, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0x2] + 0x242070db, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0x3] + 0xc1bdceee, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x4] + 0xf57c0faf, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x5] + 0x4787c62a, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0x6] + 0xa8304613, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0x7] + 0xfd469501, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x8] + 0x698098d8, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x9] + 0x8b44f7af, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0xa] + 0xffff5bb1, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0xb] + 0x895cd7be, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0xc] + 0x6b901122, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0xd] + 0xfd987193, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0xe] + 0xa679438e, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0xf] + 0x49b40821, 22);

    // round 2
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x1] + 0xf61e2562, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0x6] + 0xc040b340, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0xb] + 0x265e5a51, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x0] + 0xe9b6c7aa, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x5] + 0xd62f105d, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0xa] + 0x02441453, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0xf] + 0xd8a1e681, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x4] + 0xe7d3fbc8, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x9] + 0x21e1cde6, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0xe] + 0xc33707d6, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0x3] + 0xf4d50d87, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x8] + 0x455a14ed, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0xd] + 0xa9e3e905, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0x2] + 0xfcefa3f8, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0x7] + 0x676f02d9, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0xc] + 0x8d2a4c8a, 20);

    // round 3
    a = b + rol32((b ^ c ^ d) + a + m[0x5] + 0xfffa3942, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x8] + 0x8771f681, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0xb] + 0x6d9d6122, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0xe] + 0xfde5380c, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0x1] + 0xa4beea44, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x4] + 0x4bdecfa9, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0x7] + 0xf6bb4b60, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0xa] + 0xbebfbc70, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0xd] + 0x289b7ec6, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x0] + 0xeaa127fa, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0x3] + 0xd4ef3085, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0x6] + 0x04881d05, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0x9] + 0xd9d4d039, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0xc] + 0xe6db99e5, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0xf] + 0x1fa27cf8, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0x2] + 0xc4ac5665, 23);

    // round 4
    a = b + rol32((c ^ (b | ~d)) + a + m[0x0] + 0xf4292244, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0x7] + 0x432aff97, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0xe] + 0xab9423a7, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x5] + 0xfc93a039, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0xc] + 0x655b59c3, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0x3] + 0x8f0ccc92, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0xa] + 0xffeff47d, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x1] + 0x85845dd1, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0x8] + 0x6fa87e4f, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0xf] + 0xfe2ce6e0, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0x6] + 0xa3014314, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0xd] + 0x4e0811a1, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0x4] + 0xf7537e82, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0xb] + 0xbd3af235, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0x2] + 0x2ad7d2bb, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x9] + 0xeb86d391, 21);

    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }

  const hash = new Uint8Array(16);
  const hashv = new DataView(hash.buffer);
  hashv.setUint32(0, a0, true);
  hashv.setUint32(4, b0, true);
  hashv.setUint32(8, c0, true);
  hashv.setUint32(12, d0, true);
  return hash;

  function rol32(x, n) {
    return (x << n) | (x >>> (32 - n));
  }
}
