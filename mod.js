/* Copyright (c) 2022 exe-dealer@yandex.ru at KAGIS

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

export async function pgconnect(...optionsChain) {
  const conn = new PgConnection(computeConnectionOptions(optionsChain));
  await conn.waitReady(); // TODO .start()
  return conn;
}

// export async function pgconnect(...options) {
//   let { '.connectRetry': connectRetry, ...connOptions } = computeConnectionOptions(options);
//   // const connectRetry = [1, '1', 'on', 'true', true].includes(connectRetryRaw);
//   const startTime = Date.now();
//   for (;;) {
//     const conn = new Connection(connOptions);
//     try {
//       await conn.whenReady;
//       return conn;
//     } catch (err) {
//       const elapsedTime = Date.now() - startTime;
//       if (elapsedTime < connectRetry && (
//         _networking.canReconnect(err) ||
//         pgerrcode(err) == '57P03' // cannot_connect_now
//       )) {
//         await new Promise(resolve => setTimeout(resolve, 1000));
//         continue;
//       }

//       // const { stack } = Object.assign(Error(), { name: '\n    ...' });
//       // err.stack += stack;
//       // console.log({ stack });
//       // throw err;

//       // wrap error to keep stacktrace
//       // TODO fix PgError props
//       throw Error(err.message, { cause: err });
//     }
//   }
// }

// /** @param  {...(string|URL|PGConnectOptions)} options */
// export function pgconnect(...options) {
//   const connection = new Connection(computeConnectionOptions(options));
//   const p = Promise.resolve(connection.whenReady);
//   p.connection = connection; // https://github.com/kagis/pgwire/issues/15
//   p.catch(Boolean);
//   return p;
// }

/**
 * @param  {...PgConnectOptions} optionsChain
 * @returns {PgPool} */
export function pgpool(...optionsChain) {
  return new PgPool(computeConnectionOptions(optionsChain));
}

export function pgliteral(s) {
  if (s == null) {
    return 'NULL';
  }
  // TODO array
  return `'` + String(s).replace(/'/g, `''`) + `'`;
}

export function pgident(...segments) {
  return segments.map(it => '"' + it.replace(/"/g, '""') + '"').join('.');
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

// TODO name
export function pgerror(err) {
  if (!err) return;
  const result = err[PgError.kDetails];
  if (result) return result;
  return pgerror(err.cause);
}

// export function pgerrcode(err) {
//   while (!err) {
//     if (err[kErrorCode]) {
//       return err[kErrorCode];
//     }
//     err = err.cause;
//   }
// }

class PgError extends Error {
  static kDetails = Symbol.for('pgwire.error');
  static rethrow(err) {
    return new PgError(err[PgError.kDetails]);
  }
  constructor(errorResponse) {
    const { code, message, ...rest } = errorResponse || 0;
    const propsfmt = (
      Object.entries(rest)
      .filter(([_, v]) => v != null)
      .map(([k, v]) => k + ' ' + JSON.stringify(v))
      .join(', ')
    );
    super(message + `\n    (${propsfmt})`);
    this.name = 'PgError.' + code;
    this[PgError.kDetails] = errorResponse;
  }
}

class PgPool {
  constructor({ '.poolSize': poolSize, '.poolIdleTimeout': poolIdleTimeout, ...options }) {
    this._connections = new Set();
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
    return new PgResponse(this._queryIter(args));
  }
  async * _queryIter(args) {
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
      // TODO error can be thrown here when connection left in transaction.
      // But left-in-transaction can be caused by another error which
      // should be nested into left-in-transaction
      await this._recycleConnection(conn);
    }
  }
  async end() {
    this._ended = true;
    await Promise.all(Array.from(this._connections, conn => conn.end()));
  }
  destroy(destroyReason) {
    this._ended = true;
    this._connections.forEach(it => it.destroy(destroyReason));
    // return destroyReason;
    // TODO should keep and throw destroyReason when querying ?
  }
  _getConnection() {
    if (this._ended) {
      throw Error('pool is not usable anymore');
    }
    if (!this._poolSize) {
      return new PgConnection(this._options);
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
    const newConn = new PgConnection(this._options);
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
      await conn.end();
      return;
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
    clearTimeout(conn._poolTimeoutId);
    this._connections.delete(conn);
  }
  /** exports pgwire.pgerrcode for cases when Pool is injected as dependency
   * and we dont want to import pgwire directly for error handling code */
  errcode(...args) {
    // TODO
    // return pgerrcode(...args);
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


class PgConnection {
  constructor({ hostname, port, password, '.debug': debug, ...startupOptions }) {
    this.onnotification = null;
    this.parameters = Object.create(null); // TODO retry init
    this._debug = ['true', 'on', '1', 1, true].includes(debug);
    this._connectOptions = { hostname, port };
    this._user = startupOptions['user'];
    this._password = password;
    this._socket = null;
    this._backendKeyData = null;
    this._lastErrorResponse = null;
    this._queuedResponseChannels = [];
    this._currResponseChannel = null;
    this._transactionStatus = null;
    this._lastPullTime = 0;
    this._wakeInterval = 2000;
    this._wakeTimer = 0;
    this._rowColumns = null; // last RowDescription
    this._rowTextDecoder = new TextDecoder('utf-8', { fatal: true });
    this._rows = [];
    this._copyingOut = false;
    this._copies = [];
    this._copybuf = null;
    this._copybufn = 0;
    this._copybufSize = null;
    this._tx = new Channel();
    this._txReadable = this._tx; // _tx can be nulled after _tx.end(), but _txReadable will be available
    this._startTx = new Channel(); // TODO retry init
    this._resolveReady = null;
    this._rejectReady = null;
    this._whenReady = new Promise(this._whenReadyExecutor.bind(this));
    this._whenReady.catch(warnError);
    this._saslScramSha256 = null;
    this._destroyReason = null;
    // run background message processing
    // create StartupMessage here to validate startupOptions in constructor call stack
    this._whenDestroyed = this._ioloop(new StartupMessage({
      ...startupOptions,
      'client_encoding': 'UTF8', // TextEncoder supports UTF8 only, so hardcode and force UTF8
      // client_encoding: 'win1251',
    }));
  }
  _whenReadyExecutor(resolve, reject) {
    // When ready is resolved then it still can be rejected.
    this._resolveReady = val => {
      this._resolveReady = Boolean; // noop
      this._rejectReady = err => {
        this._whenReady = Promise.reject(err);
        this._whenReady.catch(warnError);
        this._rejectReady = Boolean; // noop
      };
      resolve(val);
    };
    // When ready is rejected then it cannot be resolved or rejected again.
    this._rejectReady = err => {
      this._resolveReady = Boolean; // noop
      this._rejectReady = Boolean; // noop
      reject(err);
    };
  }
  async waitReady() {
    try {
      await this._whenReady;
    } catch (cause) {
      if (cause instanceof PgError) {
        throw PgError.rethrow(cause);
      }
      const err = Error(cause.message);
      err.cause = cause;
      throw err;
    }
  }
  /** resolved when no quieries can be emitted and connection is about to terminate */
  get whenEnded() {
    // TODO private _whenEnded
    return this._txReadable._whenEnded;
  }
  /** number of pending queries */
  get pending() {
    return this._queuedResponseChannels.length;
  }
  get inTransaction() {
    return this._socket && ( // if not destroyed
      // TODO != 0x49 /*I*/
      this._transactionStatus == 0x45 || // E
      this._transactionStatus == 0x54 // T
    ) && this._transactionStatus; // avoid T|E value erasure
  }
  get pid() {
    if (!this._backendKeyData) return null;
    return this._backendKeyData.pid;
  }
  // TODO accept abortSignal to abort nonstream queries.
  // If abortSignal is set then we can block tx (disable pipelining)
  // to make query abortable in case of concurent queries queued.
  // We can provide .setAbortSignal method on PgResponse .
  // Is it ok to not follow AbortController convention?
  // TODO executemany?
  // Parse
  // Describe(portal)
  // Bind
  // Execute
  // Bind
  // Execute
  query(...args) {
    // TODO need to invent a way to accept abortSignal
    return new PgResponse(this._queryIter(args, null /*signal*/));
  }
  async * _queryIter(args, signal) {
    // Stack trace is broken if error occures in initial generator tick.
    // Spent many hours to understand why, but I can't.
    // Seems that this is payment for Thenable satan magic in PgResponse.
    await null;
    // `at PgResponse.then` is the lowest line of stack trace when no `await null`.
    // but if do `await null` then stack trace is proper.
    // console.trace()

    if (signal?.aborted) {
      const err = Error('postgres query aborted');
      err.cause = signal.reason;
      throw err;
    }
    if (!this._tx) {
      const err = Error('connection destroyed');
      err.cause = this._destroyReason;
      throw err;
    }
    // TODO(test) ordered queue .query() and .end() calls
    // .query after .end should throw stable error ? what if auth failed after .end

    const stdinSignal = { aborted: false, reason: Error('aborted') };
    const responseEndLock = new Channel();
    // Collect outgoing messages into intermediate array
    // to allow errors occur before any message enqueued.
    const frontendMessages = [];
    if (typeof args[0] == 'string') {
      simpleQuery(frontendMessages, args[0], args[1], stdinSignal, responseEndLock);
    } else {
      extendedQuery(frontendMessages, args, stdinSignal);
    }
    const responseChannel = new Channel();

    // TODO This two steps should be executed atomiсally.
    // If one fails then connection instance
    // will be desyncronized and should be destroyed.
    // But I see no cases when this can throw error.
    frontendMessages.forEach(this._tx.push, this._tx);
    this._queuedResponseChannels.push(responseChannel);
    try {
      for (;;) {
        // Important to call responseChannel.next() as first step of try block
        // to maximize chance that finally block will be executed when
        // query is received by server. So CancelRequest will be emited
        // in case of error, and skip-waiting duration will be minimized.
        const { value, done } = await responseChannel.next();
        if (signal?.aborted) {
          const err = Error('postgres query aborted');
          err.cause = signal.reason;
          throw err;
        }
        if (!done) {
          yield value;
          // We will send 'wake' message if _lastPullTime is older
          // than _wakeInterval, so user have a chance to break loop
          // when response is stuck.
          // perfomance.now is more suitable, but nodejs incompatible
          this._lastPullTime = Date.now();
          continue;
        }
        if (value instanceof Error) {
          const err = Error('postgres query failed');
          err.cause = value;
          throw err;
        }
        if (value) {
          throw new PgError(value);
        }
        break;
      }
    } catch (err) {
      stdinSignal.reason = err;
      throw err;
    } finally {
      // if query completed successfully then all stdins are drained,
      // so abort will have no effect and will not break anything.
      // Оtherwise
      // - if error occured
      // - or iter was .returned()
      // - or no COPY FROM STDIN was queries
      // then we should abort all pending stdins
      // stdinAbortCtl.abort();
      stdinSignal.aborted = true;

      // https://github.com/kagis/pgwire/issues/17
      if ( // going to do CancelRequest
        // if response is started and not ended
        this._currResponseChannel == responseChannel &&
         // and there are no other pending responses
         // (to prevent miscancel of next queued query)
        this._queuedResponseChannels.length == 1
      ) {
        // new queries should not be emitted during CancelRequest
        this._tx?.push(responseEndLock);
        // TODO there is a small window between first Sync and Query message
        // when query is not actually executing, so there is small chance
        // that CancelRequest may be ignored. So need to repeat CancelRequest until
        // responseChannel.return() is resolved
        await this._cancelRequest().catch(warnError);
      }
      // wait ReadyForQuery
      await responseChannel.return();
      responseEndLock.end();
    }
  }

  // should immediately close connection for new queries
  // should terminate connection gracefully
  // should be idempotent
  // should never throw, at least when used in finally block.

  /** terminates connection gracefully if possible and waits until termination complete */
  async end() {
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

    // TODO _net.close can throw
    await this._whenDestroyed;
  }
  destroy(destroyReason) {
    this._rejectReady(destroyReason || Error('connection destroyed'));
    clearInterval(this._wakeTimer);
    if (this._socket) {
      _net.close(this._socket); // TODO can throw
      this._socket = null;
      this._backendKeyData = null;
    }
    if (this._startTx) {
      this._startTx.end();
      this._startTx = null;
    }
    if (this._tx) {
      this._tx.end();
      this._tx = null;
      // accept destroyReason only if .destroy() called before .end()
      // so new queries will be rejected with the same error before
      // and after whenReady settled
      this._destroyReason = destroyReason;
    }
    // TODO await _flushData
    const responseEndReason = destroyReason || Error('incomplete response');
    while (this._queuedResponseChannels.length) {
      this._queuedResponseChannels.shift().end(responseEndReason);
    }
    this._currResponseChannel = null;
    // TODO do CancelRequest to wake stuck connection which can continue executing ?
    return destroyReason; // user code can call .destroy and throw destroyReason in one line
  }

  logicalReplication({ slot, startLsn = '0/0', options = {}, ackInterval = 10e3 }) {
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
    const stream = new ReplicationStream(rx, tx, ackInterval);
    stream.ack(startLsn); // set initial lsn and also validate lsn
    return stream;
  }
  async _cancelRequest() {
    // await this._whenReady; // wait for backendkey
    if (!this._backendKeyData) {
      throw Error('CancelRequest attempt before BackendKeyData received');
    }
    const socket = await this._createSocket();
    try {
      const m = new CancelRequest(this._backendKeyData);
      await _net.write(socket, serializeFrontendMessage(m));
    } finally {
      _net.close(socket);
    }
  }
  async _ioloop(startupmsg) {
    let caughtError;
    try {
      this._socket = await this._createSocket();
      this._startTx.push(startupmsg);
      await Promise.all([ // allSettled ?
        // TODO при досрочном завершении одной из функций как будем завершать вторую ?
        // _sendMessages надо завершать потому что она может пайпить stdin,
        // а недокушаный stdin надо прибить

        // когда может обломаться recvMessages
        // - сервер закрыл сокет
        // - сервер прислал херню
        // - pgwire не смог обработать авторизационные сообщения
        this._recvMessages(),
        // TODO this._socket.closeWrite when _pipeMessages complete
        this._pipeMessages(this._startTx),
      ]);
    } catch (err) {
      caughtError = err;
    }
    this.destroy(caughtError);
  }
  async _createSocket() {
    const socket = await _net.connect(this._connectOptions);
    if (!this._tls) return socket;
    // TODO implement tls
    try {
      await _net.write(socket, serializeFrontendMessage(new SSLRequest()));
      const sslResp = await readByte(socket);
      if (sslResp == 'S') {
        return await _net.startTls(socket, { });
      }
      if (this._tls == 'require') {
        // TODO error message
        throw Error('postgres refuses secure connection');
      }
      return socket;
    } catch (err) {
      _net.close(socket);
      throw err;
    }
  }
  async _pipeMessages(from) {
    const iter = from[Symbol.asyncIterator]();
    try {
      for (let value, done = false; !done; ) {
        [{ value, done }] = await Promise.all([
          iter.next(),
          value && this._sendMessage(value),
        ]);
      }
    } catch (err) {
      // need to destroy connection here to prevent deadlock.
      // If _sendMessage throw error than startTx.next() is still
      // unresolved and waiting for server to respond or close connection.
      // So iter.return() also will not be resolved
      throw this.destroy(err);
    } finally {
      await iter.return();
    }
  }
  async _sendMessage(m) {
    if (m[Symbol.asyncIterator]) {
      return this._pipeMessages(m);
    }
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
    await _net.write(this._socket, serializeFrontendMessage(m));
  }
  async _recvMessages() {
    for await (const { nparsed, messages } of BackendMessageReader.iterBackendMessages(this._socket)) {
      // Use last backend messages chunk size as _copybuf size hint.
      this._copybufSize = nparsed;
      this._copybuf = null;
      this._copybufn = 0;
      for (const m of messages) {
        if (this._debug) {
          console.log(... m.payload === undefined
            ? ['-> %s', m.tag]
            : ['-> %s %o', m.tag, m.payload],
          );
        }
        // TODO check if connection is destroyed to prevent errors in _recvMessage?
        const maybePromise = this._recvMessage(m);
        // avoid await for DataRow and CopyData messages for perfomance
        if (maybePromise) {
          await maybePromise;
        }
      }
      await this._flushDataRows();
      await this._flushCopyDatas();
    }
    if (this._lastErrorResponse) {
      throw new PgError(this._lastErrorResponse);
    }
    // TODO handle unexpected connection close when _pipeMessages still working.
    // if _pipeMessages(this._startTx) is not resolved yet
    // then _ioloop will not be resolved. But there no more active socket
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

      case 'NotificationResponse': return this._recvNotificationResponse(m, m.payload);
    }
  }
  _recvDataRow(_, /** @type {Array<Uint8Array>} */ row) {
    for (let i = 0; i < this._rowColumns.length; i++) {
      const valbuf = row[i];
      if (valbuf == null) continue;
      const { binary, typeOid } = this._rowColumns[i];
      // TODO avoid this._clientTextDecoder.decode for bytea
      row[i] = (
        binary
          // do not valbuf.slice() because nodejs Buffer .slice does not copy
          // TODO but we not going to receive Buffer here ?
          ? Uint8Array.prototype.slice.call(valbuf)
          : typeDecode(this._rowTextDecoder.decode(valbuf), typeOid)
      );
    }
    this._rows.push(row);
  }
  _recvCopyData(_, /** @type {Uint8Array} */ data) {
    if (!this._copyingOut) {
      // postgres can send CopyData when not in copy out mode, steps to reproduce:
      //
      // <- StartupMessage({ database: 'postgres', user: 'postgres', replication: 'database' })
      // <- Query('CREATE_REPLICATION_SLOT a LOGICAL test_decoding')
      // wait for ReadyForQuery
      // <- Query('START_REPLICATION SLOT a LOGICAL 0/0');
      // <- CopyDone() before wal_sender_timeout expires
      // -> CopyData keepalive message can be received here after CopyDone but before ReadyForQuery
      //
      // <- Query('CREATE_REPLICATION_SLOT b LOGICAL test_decoding')
      // -> CopyData keepalive message sometimes can be received here before RowDescription message
      return;
    }
    if (!this._copybuf) {
      this._copybuf = new Uint8Array(this._copybufSize);
    }
    this._copybuf.set(data, this._copybufn);
    this._copies.push(this._copybuf.subarray(this._copybufn, this._copybufn + data.length));
    this._copybufn += data.length;
  }
  async _flushCopyDatas() {
    if (!this._copies.length) return;
    const copyChunk = new PgResponseChunk(this._copybuf.buffer, this._copybuf.byteOffset, this._copybufn);
    copyChunk.tag = 'CopyData';
    copyChunk.copies = this._copies;
    this._copybuf = this._copybuf.subarray(this._copybufn);
    this._copybufn = 0;
    this._copies = [];
    await this._currResponseChannel.push(copyChunk);
  }
  async _flushDataRows() {
    if (!this._rows.length) return;
    const rowsChunk = new PgResponseChunk();
    rowsChunk.tag = 'DataRow';
    rowsChunk.rows = this._rows;
    this._rows = [];
    await this._currResponseChannel.push(rowsChunk);
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
  async _recvRowDescription(m, columns) {
    this._rowColumns = columns;
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
    // So it should be possible to provide password as Uint8Array
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
    if (this._startTx) {
      return this._completeStartup();
    }
    if (this._currResponseChannel) {
      await this._endResponse(m);
    } else {
      await this._startResponse(m);
    }
  }
  _completeStartup() {
    this._startTx.push(this._txReadable);
    this._startTx.end();
    this._startTx = null;
    this._resolveReady();
    this._wakeTimer = setInterval(this._wake.bind(this), this._wakeInterval);
  }
  async _startResponse(m) {
    this._currResponseChannel = this._queuedResponseChannels[0];
    // TODO assert this._currResponseChan is not null
    await this._fwdBackendMessage(m);
  }
  async _endResponse() {
    // await this._fwdBackendMessage(m);
    this._queuedResponseChannels.shift();
    this._currResponseChannel.end(this._lastErrorResponse);
    this._currResponseChannel = null;
    this._lastErrorResponse = null;
  }

  async _recvNoticeResponse(m) {
    if (this._currResponseChannel) {
      return this._fwdBackendMessage(m);
    }
    // TODO dispatchEvent for nonquery NoticeResponse
  }
  _recvParameterStatus(_, { parameter, value }) {
    this.parameters[parameter] = value;
    // TODO emit event ?
  }
  _recvBackendKeyData(_, backendKeyData) {
    this._backendKeyData = backendKeyData;
  }
  _recvNotificationResponse(_, payload) {
    // if onnotification throw error in _ioloop then error will be swallowed
    Promise.resolve(payload).then(this.onnotification);
  }
  async _fwdBackendMessage({ tag, payload }) {
    await this._flushDataRows();
    await this._flushCopyDatas();
    const chunk = new PgResponseChunk();
    chunk.tag = tag;
    chunk.payload = payload;
    await this._currResponseChannel.push(chunk);
  }
  _wake() {
    if (!this._currResponseChannel?.drained) return;
    if (this._lastPullTime + this._wakeInterval > Date.now()) return;
    const wakeChunk = new PgResponseChunk();
    wakeChunk.tag = 'wake';
    this._currResponseChannel.push(wakeChunk);
  }
}

class PgResponseChunk extends Uint8Array {
  tag;
  payload = null;
  rows = [];
  copies = [];
}

function warnError(err) {
  // console.error('warning', err);
}

function simpleQuery(out, script, { stdin, stdins = [] } = {}, stdinSignal, responseEndLock) {
  // To cancel query we should send CancelRequest _after_
  // query is received by server and started executing.
  // But we cannot wait for first chunk because postgres can hold it
  // for an unpredictably long time. I see no way to make postgres
  // to flush RowDescription in simple protocol. So we prepend Query message
  // with Sync to eagerly know when query is started and can be cancelled.
  // Also it makes possible to determine whether NoticeResponse/ErrorResponse
  // is asyncronous server message or belongs to query.
  // (TODO Maybe there is a small window between first ReadyForQuery and actual
  // query execution, when async messages can be received. Need more source digging)
  // Seems that this is ok to do Sync during simple protocol
  // even when replication=database.
  out.push(new Sync());
  out.push(new Query(script));
  // TODO handle case when number of stdins is unknown
  // should block tx and wait for CopyInResponse/CopyBothResponse
  if (stdin) {
    out.push(wrapCopyData(stdin, stdinSignal));
  }
  for (const stdin of stdins) {
    out.push(wrapCopyData(stdin, stdinSignal));
  }
  // when CREATE_REPLICATION_SLOT or START_REPLICATION is emitted
  // then no other queries should be emmited until ReadyForQuery is received.
  // Seems that its a postgres server bug.
  // This workaround looks like fragile hack but its not.
  // Its dirty but safe enough because no comments or other statements can
  // precede CREATE_REPLICATION_SLOT or START_REPLICATION
  if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(script)) {
    out.push(responseEndLock);
  } else {
    out.push(new CopyFail('missing copy upstream'));
  }
}
function extendedQuery(out, blocks, stdinSignal) {
  out.push(new Sync()); // see top comment in simpleQuery
  for (const m of blocks) {
    switch (m.message) {
      case undefined:
      case null: extendedQueryStatement(out, m, stdinSignal); break;
      case 'Parse': extendedQueryParse(out, m); break;
      case 'Bind': extendedQueryBind(out, m); break;
      case 'Execute': extendedQueryExecute(out, m, stdinSignal); break;
      case 'DescribeStatement': extendedQueryDescribeStatement(out, m); break;
      case 'CloseStatement': extendedQueryCloseStatement(out, m); break;
      case 'DescribePortal': extendedQueryDescribePortal(out, m); break;
      case 'ClosePortal': extendedQueryClosePortal(out, m); break;
      case 'Flush': out.push(new Flush()); break;
      default: throw Error('unknown extended message ' + JSON.stringify(m.message));
    }
  }
  out.push(new Sync());
}
function extendedQueryStatement(out, { statement, params = [], limit, binary, stdin, noBuffer }, stdinSignal) {
  const paramTypes = params.map(({ type }) => type);
  extendedQueryParse(out, { statement, paramTypes });
  extendedQueryBind(out, { params, binary });
  extendedQueryExecute(out, { limit, stdin, noBuffer }, stdinSignal);
  // if (noBuffer) {
  //   out.push(new Flush());
  // }
}
function extendedQueryParse(out, { statement, statementName, paramTypes = [] }) {
  const paramTypeOids = paramTypes.map(typeResolve);
  out.push(new Parse({ statement, statementName, paramTypeOids }));
}
function extendedQueryBind(out, { portal, statementName, binary, params = [] }) {
  params = params.map(encodeParam);
  out.push(new Bind({ portal, statementName, binary, params }));

  function encodeParam({ value, type}) {
    // treat Uint8Array values as already encoded,
    // so user can receive value with unknown type as Uint8Array
    // from extended .query and pass it back as parameter
    // it also "encodes" bytea in efficient way instead of hex
    if (value instanceof Uint8Array) return value;
    return typeEncode(value, typeResolve(type));
  }
}
function extendedQueryExecute(out, { portal, stdin, limit, noBuffer }, stdinSignal) {
  // TODO write test to explain why
  // we need unconditional DescribePortal
  // before Execute
  out.push(new DescribePortal(portal));
  out.push(new Execute({ portal, limit }));
  // if (noBuffer) {
  //   out.push(new Flush());
  // }
  if (stdin) {
    out.push(wrapCopyData(stdin, stdinSignal));
  } else {
    // CopyFail message ignored by postgres
    // if there is no COPY FROM statement
    out.push(new CopyFail('missing copy upstream'));
  }
  // TODO nobuffer option
  // yield new Flush();
}
function extendedQueryDescribeStatement(out, { statementName }) {
  out.push(new DescribeStatement(statementName));
}
function extendedQueryCloseStatement(out, { statementName }) {
  out.push(new CloseStatement(statementName));
}
function extendedQueryDescribePortal(out, { portal }) {
  out.push(new DescribePortal(portal));
}
function extendedQueryClosePortal({ portal }) {
  out.push(new ClosePortal(portal));
}
async function * wrapCopyData(source, signal) {
  // TODO dry
  // if (abortSignal.aborted) {
  //   return;
  // }
  try {
    for await (const chunk of source) {
      if (signal.aborted) {
        throw signal.reason;
      }
      yield new CopyData(chunk);
    }
    yield new CopyDone();
  } catch (err) {
    // FIXME err.stack lost
    // store err
    // do CopyFail (copy_error_key)
    // rethrow stored err when ErrorResponse received
    yield new CopyFail(String(err));
  }
}

class PgResponse {
  /** @type {Promise<PgResult>} */
  _loadPromise;
  _iter;

  constructor(iter) {
    this._iter = iter;
  }
  [Symbol.asyncIterator]() {
    // TODO activate iterator (emit query)?
    // it can be handy when we need to enqueue .end after .query immediately
    // or if need to enqueue .query('discard all') after user .query in pool
    return this._iter[Symbol.asyncIterator]();
  }

  // make PgResponse behave like lazy promise
  then(...args) { return this._loadOnce().then(...args); }
  catch(...args) { return this._loadOnce().catch(...args); }
  finally(...args) { return this._loadOnce().finally(...args); }
  _loadOnce() { return this._loadPromise || (this._loadPromise = this._load()); }

  async _load() {
    const notices = []
    const results = [];
    let result = new PgSubResult();
    for await (const chunk of this._iter) {
      result.rows.push(...chunk.rows);
      // lastResult.copies.push(...chunk.copies); // TODO chunk.copies
      switch (chunk.tag) {
        // TODO NoData
        // TODO ParameterDescription
        // TODO CopyOutResponse

        case 'NoticeResponse':
          notices.push(chunk.payload);
          continue;
        // case 'CopyOutResponse':
        // case 'CopyBothResponse':
        //   lastResult.columns = chunk.payload.columns;
        //   break;
        case 'RowDescription':
          result.columns = chunk.payload;
          continue;
        default:
          continue;

        // statement result boundaries
        case 'CommandComplete':
          result.status = chunk.payload;
          break;
        case 'PortalSuspended':
        case 'EmptyQueryResponse':
          result.status = chunk.tag;
          break;
      }
      results.push(result);
      result = new PgSubResult();
    }
    return new PgResult(results, notices);
  }
}

class PgResult {
  constructor(results, notices) {
    /** @type {PgSubResult[]} */
    this.results = results;
    this.notices = notices;
  }
  /** @deprecated */
  get scalar() {
    const [scalar] = this;
    return scalar;
  }
  get rows() {
    return this._lastSelect?.rows || [];
  }
  get columns() {
    return this._lastSelect?.columns || [];
  }
  get status() {
    if (this.results.length == 1) {
      return this.results[0].status;
    }
  }
  * [Symbol.iterator]() {
    const [row] = this.rows;
    if (row) yield * row;
  }
  get _lastSelect() {
    for (let i = this.results.length - 1; i >= 0; i--) {
      if (!this.results[i].columns.length) continue;
      return this.results[i];
    }
  }
}

class PgSubResult {
  /** @type {any[][]} */
  rows = [];
  columns = [];
  status = null;
  /** @deprecated */
  get scalar() { return this.rows[0]?.[0]; }
}

class ReplicationStream {
  constructor(rx, tx, ackInterval) {
    this._rx = rx;
    /** @type {Channel} */
    this._tx = tx;
    this._ackInterval = ackInterval;
    this._ackingLsn = '00000000/00000000';
    this._ackmsg = Uint8Array.of(
      0x72, // r
      0, 0, 0, 0, 0, 0, 0, 0, // 1 - written lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 9 - flushed lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 17 - applied lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 25 - time
      0, // 33 - should reply
    );
    this._ackmsgWrittenLsn = new DataView(this._ackmsg.buffer, 1, 8);
    this._ackmsgFlushedLsn = new DataView(this._ackmsg.buffer, 9, 8);
    this._ackmsgAppliedLsn = new DataView(this._ackmsg.buffer, 17, 8);
    this._iter = this._iterate(); // TODO can use `this` before all props inited? (hidden class optimization)
  }
  /** @returns {AsyncIterable<PgoChunk>} */
  pgoutputDecode() {
    return PgoutputReader.decodeStream(this);
  }
  [Symbol.asyncIterator]() {
    return this._iter;
  }
  async * _iterate() {
    let ackTimer;
    try {
      const msgreader = new ReplicationMessageReader();
      let lastLsn = '';
      let lastTime = 0n;
      for (;;) {
        const { value: chunk, done } = await this._rx.next();
        if (done) break;
        const messages = [];
        let shouldAck = false;
        for (const copyData of chunk.copies) {
          msgreader.reset(copyData);
          const msg = msgreader.readReplicationMessage();
          switch (msg.tag) {
            case 'XLogData':
              messages.push(msg);
              break;
            case 'PrimaryKeepaliveMessage':
              shouldAck = shouldAck || msg.shouldReply;
              break;
          }
          if (lastLsn < msg.lsn) lastLsn = msg.lsn;
          if (lastLsn < msg.endLsn) lastLsn = msg.endLsn;
          if (lastTime < msg.time) lastTime = msg.time;
        }
        if (shouldAck) {
          this._ackImmediate();
        }
        // Start yielding messages only when replication started succesfully.
        // First message on successful start will be PrimaryKeepaliveMessage.
        // Skip CopyBothResponse, seems that there is no usefull information.
        // TODO but we loose ability to break loop if replication initialization is stuck.
        if (lastLsn) {
          ackTimer = ackTimer || setInterval(this._ackImmediate.bind(this), this._ackInterval);
          yield { lastLsn, lastTime, messages };
        }
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
    let nlsn = BigInt('0x' + this._ackingLsn.replace('/', ''));
    nlsn += 1n;
    // TODO accept { written, flushed, applied, immediate }
    this._ackmsgWrittenLsn.setBigUint64(0, nlsn);
    this._ackmsgFlushedLsn.setBigUint64(0, nlsn);
    this._ackmsgAppliedLsn.setBigUint64(0, nlsn);
  }
  _ackImmediate() {
    if (!this._tx.drained) return;
    this._tx.push(this._ackmsg);
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
    // TODO string size is depends on encoder
    const sizeCounter = new MessageSizer();
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
  _write(w, size, { statement, statementName = '', paramTypeOids = [] }) {
    w.writeUint8(0x50); // P
    w.writeInt32BE(size - 1);
    w.writeString(statementName);
    w.writeString(statement);
    w.writeInt16BE(paramTypeOids.length);
    for (const typeOid of paramTypeOids) {
      w.writeUint32BE(typeOid || 0);
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
      w.writeBindParam(p);
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
  /** @type {Uint8Array} */
  _b = null;
  _p = 0;
  // should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
  static defaultTextDecoder = new TextDecoder();
  _textDecoder = MessageReader.defaultTextDecoder;

  reset(/** @type {Uint8Array} */ b) {
    this._b = b;
    this._p = 0;
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
  /**
   * @template T
   * @param {() => T} fn
   * @return {T[]} */
  _array(length, fn) {
    return Array.from({ length }, fn, this);
  }
  // replication helpers
  readLsn() {
    const h = this.readUint32(), l = this.readUint32();
    if (h == 0 && l == 0) return null;
    return (
      h.toString(16).padStart(8, '0') + '/' +
      l.toString(16).padStart(8, '0')
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

// https://www.postgresql.org/docs/14/protocol-message-formats.html
class BackendMessageReader extends MessageReader {
  static async * iterBackendMessages(socket) {
    const msgreader = new BackendMessageReader();
    let buf = new Uint8Array(16_640);
    let nbuf = 0;
    for (;;) {
      if (nbuf >= buf.length) { // grow buffer
        const oldbuf = buf;
        buf = new Uint8Array(oldbuf.length * 2); // TODO prevent uncontrolled grow
        buf.set(oldbuf);
      }
      const nread = await _net.read(socket, buf.subarray(nbuf));
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
        // TODO use grow hint
        if (nbuf < inext) break; // incomplete message
        msgreader.reset(buf.subarray(ipayload, inext));
        const message = msgreader._readBackendMessage(buf[itag]);
        messages.push(message);
        // TODO batch DataRow here
        nparsed = inext;
      }
      yield { nparsed, messages };

      if (nparsed) { // TODO check if copyWithin(0, 0) is noop
        buf.copyWithin(0, nparsed, nbuf); // move unconsumed bytes to begining of buffer
        nbuf -= nparsed;
      }
    }
  }

  /**
   * @template {string} T
   * @template P
   * @param {T} tag
   * @param {P} payload
   */
  _m(tag, payload) {
    return { tag, payload };
  }
  _readBackendMessage(asciiTag) {
    const m = this._m;
    switch (asciiTag) {
      case 0x64 /*d*/: return m('CopyData', this._b);
      case 0x44 /*D*/: return m('DataRow', this._readDataRow());
      case 0x76 /*v*/: return m('NegotiateProtocolVersion', this._readNegotiateProtocolVersion());
      case 0x53 /*S*/: return m('ParameterStatus', this._readParameterStatus());
      case 0x4b /*K*/: return m('BackendKeyData', this._readBackendKeyData());
      case 0x5a /*Z*/: return m('ReadyForQuery', this._readReadyForQuery());
      case 0x48 /*H*/: return m('CopyOutResponse', this._readCopyResponse());
      case 0x47 /*G*/: return m('CopyInResponse', this._readCopyResponse());
      case 0x57 /*W*/: return m('CopyBothResponse', this._readCopyResponse());
      case 0x63 /*c*/: return m('CopyDone');
      case 0x54 /*T*/: return m('RowDescription', this._readRowDescription());
      case 0x74 /*t*/: return m('ParameterDescription', this._readParameterDescription());
      case 0x43 /*C*/: return m('CommandComplete', this.readString());
      case 0x31 /*1*/: return m('ParseComplete');
      case 0x32 /*2*/: return m('BindComplete');
      case 0x33 /*3*/: return m('CloseComplete');
      case 0x73 /*s*/: return m('PortalSuspended');
      case 0x49 /*I*/: return m('EmptyQueryResponse');
      case 0x4e /*N*/: return m('NoticeResponse', this._readErrorOrNotice());
      case 0x45 /*E*/: return m('ErrorResponse', this._readErrorOrNotice());
      case 0x6e /*n*/: return m('NoData');
      case 0x41 /*A*/: return m('NotificationResponse', this._readNotificationResponse());

      case 0x52 /*R*/: switch (this.readInt32()) {
        case 0       : return m('AuthenticationOk');
        case 2       : return m('AuthenticationKerberosV5');
        case 3       : return m('AuthenticationCleartextPassword');
        case 5       : return m('AuthenticationMD5Password', { salt: this.read(4) });
        case 6       : return m('AuthenticationSCMCredential');
        case 7       : return m('AuthenticationGSS');
        case 8       : return m('AuthenticationGSSContinue', this.readToEnd());
        case 9       : return m('AuthenticationSSPI');
        case 10      : return m('AuthenticationSASL', { mechanism: this.readString() });
        case 11      : return m('AuthenticationSASLContinue', this.readToEnd());
        case 12      : return m('AuthenticationSASLFinal', this.readToEnd());
        default      : throw Error('unknown auth message');
      }
      default: throw Error(`unsupported backend message ${asciiTag}`);
    }
  }
  _readDataRow() {
    const nfields = this.readInt16()
    const row = Array(nfields);
    for (let i = 0; i < nfields; i++) {
      const valsize = this.readInt32();
      row[i] = valsize < 0 ? null : this.read(valsize);
    }
    return row;
  }
  _readNegotiateProtocolVersion() {
    const version = this.readInt32();
    const unrecognizedOptions = this._array(this.readInt32(), this.readString);
    return { version, unrecognizedOptions };
  }
  _readParameterDescription() {
    return this._array(this.readInt16(), this.readInt32);
  }
  _readBackendKeyData() {
    const pid = this.readInt32();
    const secretKey = this.readInt32();
    return { pid, secretKey };
  }
  _readReadyForQuery() {
    return { transactionStatus: this.readUint8() };
  }
  _readCopyResponse() {
    const binary = this.readUint8();
    const columns = this._array(this.readInt16(), _ => ({
      binary: this.readInt16(),
    }));
    // TODO names
    return { binary, columns };
  }
  _readRowDescription() {
    return this._array(this.readInt16(), _ => ({
      name: this.readString(),
      tableOid: this.readInt32(),
      tableColumn: this.readInt16(),
      typeOid: this.readInt32(),
      typeSize: this.readInt16(),
      typeMod: this.readInt32(),
      binary: this.readInt16(),
    }));
  }
  _readParameterStatus() {
    const parameter = this.readString();
    const value = this.readString();
    return { parameter, value };
  }
  _readErrorOrNotice() {
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
  _readNotificationResponse() {
    const pid = this.readInt32();
    const channel = this.readString();
    const payload = this.readString();
    return { pid, channel, payload };
  }
}

// https://www.postgresql.org/docs/14/protocol-replication.html#id-1.10.5.9.7.1.5.1.8
class ReplicationMessageReader extends MessageReader {
  readReplicationMessage() {
    const tag = this.readUint8();
    switch (tag) {
      case 0x77 /*w*/: return this._readXLogData();
      case 0x6b /*k*/: return this._readPrimaryKeepaliveMessage();
      default: throw Error('unknown replication message');
    }
  }
  _readXLogData() {
    return {
      tag: /** @type {const} */ ('XLogData'),
      lsn: this.readLsn(),
      // `endLsn` is always the same as `lsn` in case of logical replication.
      // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1240
      endLsn: this.readLsn(),
      // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1270-L1271
      time: this.readTime(),
      data: this.readToEnd(),
    };
  }
  _readPrimaryKeepaliveMessage() {
    return {
      tag: /** @type {const} */ ('PrimaryKeepaliveMessage'),
      lsn: null, // hidden class opt
      endLsn: this.readLsn(),
      time: this.readTime(),
      shouldReply: this.readUint8(),
    };
  }
}

// https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html
class PgoutputReader extends MessageReader {
  static async * decodeStream(replstream) {
    const pgoreader = new PgoutputReader();
    for await (const chunk of replstream) {
      for (const msg of chunk.messages) {
        pgoreader.reset(msg.data);
        pgoreader._upgradeMsg(msg);
        msg.data = null; // free XLogData buffer
      }
      yield chunk;
    }
  }

  /** @type {Map<number, { typeSchema: string, typeName: string }>} */
  _typeCache = new Map();
  _relationCache = new Map();

  _upgradeMsg(out) {
    const tag = this.readUint8();
    switch (tag) {
      case 0x42 /*B*/: return this._upgradeMsgBegin(out);
      case 0x4f /*O*/: return this._upgradeMsgOrigin(out);
      case 0x59 /*Y*/: return this._upgradeMsgType(out);
      case 0x52 /*R*/: return this._upgradeMsgRelation(out);
      case 0x49 /*I*/: return this._upgradeMsgChange(out, 'insert', true);
      case 0x55 /*U*/: return this._upgradeMsgChange(out, 'update', true);
      case 0x44 /*D*/: return this._upgradeMsgChange(out, 'delete', false);
      case 0x54 /*T*/: return this._upgradeMsgTruncate(out);
      case 0x4d /*M*/: return this._upgradeMsgMessage(out);
      case 0x43 /*C*/: return this._upgradeMsgCommit(out);
      default: throw Error('unknown pgoutput message');
    }
  }
  _upgradeMsgBegin(out) {
    // TODO lsn can be null if origin sended
    // https://github.com/postgres/postgres/blob/85c61ba8920ba73500e1518c63795982ee455d14/src/backend/replication/pgoutput/pgoutput.c#L409
    out.tag = 'begin';
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275
    out.commitLsn = this.readLsn();
    out.commitTime = this.readTime();
    out.xid = this.readInt32();
  }
  _upgradeMsgOrigin(out) {
    out.tag = 'origin';
    out.originLsn = this.readLsn();
    out.originName = this.readString();
  }
  _upgradeMsgType(out) {
    out.tag = 'type';
    out.typeOid = this.readInt32();
    out.typeSchema = this.readString();
    out.typeName = this.readString();
    // mem leak not likely to happen because amount of types is usually small
    this._typeCache.set(out.typeOid, {
      typeSchema: out.typeSchema,
      typeName: out.typeName,
    });
  }
  _upgradeMsgRelation(out) {
    // lsn expected to be null
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/replication/walsender.c#L1342
    out.tag = 'relation';
    out.relationOid = this.readInt32();
    out.schema = this.readString();
    out.name = this.readString();
    out.replicaIdentity = this._readRelationReplicaIdentity();
    out.attrs = this._array(this.readInt16(), this._readRelationAttr);
    out.keyNames = out.attrs.filter(it => it.flags & 0b1).map(it => it.name);
    // mem leak not likely to happen because amount of relations is usually small
    this._relationCache.set(out.relationOid, out);
  }
  _readRelationReplicaIdentity() {
    // https://www.postgresql.org/docs/14/catalog-pg-class.html
    const relreplident = this.readUint8();
    switch (relreplident) {
      case 0x64 /*d*/: return 'default';
      case 0x6e /*n*/: return 'nothing';
      case 0x66 /*f*/: return 'full';
      case 0x69 /*i*/: return 'index';
      default: return relreplident;
    }
  }
  _readRelationAttr() {
    const attr = {
      flags: this.readUint8(),
      name: this.readString(),
      typeOid: this.readInt32(),
      typeMod: this.readInt32(),
      typeSchema: null,
      typeName: null, // TODO resolve builtin type names?
    }
    Object.assign(attr, this._typeCache.get(attr.typeOid));
    return attr;
  }
  _upgradeMsgChange(out, tag, readN) {
    const relid = this.readInt32();
    const relation = this._relationCache.get(relid);
    const actionKON = this.readUint8();
    const keyOnly = actionKON == 0x4b /*K*/;
    let before = this._readTuple(relation);
    let after = null;
    if (actionKON == 0x4e /*N*/) {
      after = before;
      before = null;
    } else if (readN) {
      const actionN = this.readUint8();
      // TODO assert actionN == 'N'
      after = this._readTuple(relation, before);
    }
    let key = before || after;
    if (relation.keyNames.length < relation.attrs.length) {
      const tup = key;
      key = Object.create(null);
      for (const k of relation.keyNames) {
        key[k] = tup[k];
      }
    }
    if (keyOnly) {
      before = null;
    }
    out.tag = tag;
    out.relation = relation;
    out.key = key;
    out.before = before;
    out.after = after;
  }
  _readTuple({ attrs }, unchangedToastFallback) {
    const nfields = this.readInt16();
    const tuple = Object.create(null);
    for (let i = 0; i < nfields; i++) {
      const { name, typeOid } = attrs[i];
      const kind = this.readUint8();
      switch (kind) {
        case 0x62 /*b*/:
          const bsize = this.readInt32();
          const bval = this.read(bsize);
          // dont need to .slice() because new buffer
          // is created for each replication chunk
          tuple[name] = bval;
          break;
        case 0x74 /*t*/:
          const valsize = this.readInt32();
          const valbuf = this.read(valsize);
          // TODO lazy decode
          // https://github.com/kagis/pgwire/issues/16
          const valtext = this._textDecoder.decode(valbuf);
          tuple[name] = typeDecode(valtext, typeOid);
          break;
        case 0x6e /*n*/:
          tuple[name] = null;
          break;
        case 0x75 /*u*/:
          tuple[name] = unchangedToastFallback?.[name] ?? undefined;
          break;
        default: throw Error(`uknown attribute kind ${kind}`);
      }
    }
    return tuple;
  }
  _upgradeMsgTruncate(out) {
    const nrels = this.readInt32();
    out.tag = 'truncate';
    out.flags = this.readUint8();
    out.cascade = Boolean(out.flags & 0b1);
    out.restartIdentity = Boolean(out.flags & 0b10);
    out.relations = this._array(nrels, _ => this._relationCache.get(this.readInt32()));
  }
  _upgradeMsgMessage(out) {
    out.tag = 'message';
    out.flags = this.readUint8();
    out.transactional = Boolean(out.flags & 0b1);
    out.messageLsn = this.readLsn();
    out.prefix = this.readString();
    out.content = this.read(this.readInt32());
  }
  _upgradeMsgCommit(out) {
    out.tag = 'commit';
    out.flags = this.readUint8(); // reserved unused
    // should be the same as begin.commitLsn,
    // postgres somehow uses it to synchronize initial dump with slot position.
    out.commitLsn = this.readLsn();
    // `out.commitEndLsn` is redundant because it always the same as `out.lsn` .
    // Here we see that ctx->write_location = txn->end_lsn
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/logical.c#L819
    // Here we see that ctx->write_location is used for `out.lsn` field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/logical.c#L634
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1239
    // And here we see that txn->end_lsn is used for `out.commitEndLsn` field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/proto.c#L87
    // Seems that they include `out.commitEndLsn` into pgoutput message to simplify `apply_handle_commit` code
    // so it can be independent of `out.lsn`, which is lower level XLogData message field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/worker.c#L780
    out.commitEndLsn = this.readLsn();
    out.commitTime = this.readTime();
  }
}


// TODO no DataView
class MessageWriter {
  static defaultTextEncoder = new TextEncoder();

  constructor(buf) {
    this._buf = buf
    this._pos = 0;
    this._textEncoder = MessageWriter.defaultTextEncoder;
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
    this.write(val);
    this.writeUint8(0);
  }
  writeBindParam(val) {
    this.writeInt32BE(0); // size prefix, will update later
    const pos = this._pos;
    this.write(val);
    const size = this._pos - pos;
    // update size prefix
    new DataView(this._buf, pos - 4).setInt32(0, size);
  }
  write(val) {
    if (val instanceof Uint8Array) {
      return this._writeUint8Array(val);
    }
    if (typeof val == 'string') {
      return this._writeString(val);
    }
    throw TypeError('string or Uint8Array expected');
  }
  _writeUint8Array(val) {
    new Uint8Array(this._buf, this._pos).set(val);
    this._pos += val.length;
  }
  _writeString(val) {
    const { read, written } = this._textEncoder.encodeInto(
      val,
      new Uint8Array(this._buf, this._pos),
    );
    if (read < val.length) {
      throw Error('too small buffer');
    }
    this._pos += written;
  }
}

class MessageSizer {
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
    // do write before zero check to validate val type first
    this.write(val);
    const z = val instanceof Uint8Array ? 0x0 : '\0';
    if (val.indexOf(z) > 0) {
      throw TypeError('zero char is not allowed');
    }
    this.writeUint8(0);
  }
  writeBindParam(val) {
    this.writeInt32BE(0);
    this.write(val);
  }
  write(val) { // TODO accept this._textEncoder
    if (val instanceof Uint8Array) {
      this.result += val.length;
    } else if (typeof val == 'string') {
      // TODO count length with this._textEncoder
      this.result += MessageSizer._utf8length(val);
    } else {
      throw TypeError('string or Uint8Array expected');
    }
  }

  // https://stackoverflow.com/a/25994411
  static _utf8length(s) {
    let result = 0;
    for (let i = 0; i < s.length; i++) {
      const c = s.charCodeAt(i);
      result += c >> 11 ? 3 : c >> 7 ? 2 : 1;
    }
    return result;
  }
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

function typeEncode(value, typeid) {
  switch (typeid) { // add line here when register new type (optional)
    case  114 /* json    */:
    case 3802 /* jsonb   */: return JSON.stringify(value);
    case   17 /* bytea   */: return typeEncodeBytea(value); // bytea encoder is used only for array element encoding
  }
  let elemTypeid = typeOfElem(typeid);
  if (elemTypeid) {
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
    return this == arr && elem != null ? typeEncode(elem, elemTypeid) : elem;
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

function typeEncodeBytea(bytes) {
  return '\\x' + Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
}

function lsnMakeComparable(text) {
  const [h, l] = text.split('/');
  return h.padStart(8, '0') + '/' + l.padStart(8, '0');
}

///////////// end type codec


class Channel {
  constructor() {
    this._qnext = []
    this._qpush = [];
    this._value = undefined;
    this._pausePushAsync = resolve => this._qpush.push({ done: false, value: this._value, resolve });
    this._pauseNextAsync = resolve => this._qnext.push(resolve);
    this._resolveEnded = null;
    this._whenEnded = new Promise(resolve => this._resolveEnded = resolve);
  }
  /** `true` if subsequent .push will immediately pass value to awaiting .next  */
  get drained() {
    return Boolean(this._qnext?.length);
  }
  async push(value) {
    if (!this._qnext) throw Error('push after ended');
    const resumeNext = this._qnext.shift();
    if (resumeNext) return resumeNext({ done: false, value });
    if (!this._qpush) return; // iterator returned
    this._value = value;
    await new Promise(this._pausePushAsync);
  }
  end(value) {
    if (!this._qnext) throw Error('already ended');
    this._value = value;
    for (const resumeNext of this._qnext) {
      resumeNext({ done: true, value });
      // TODO return { value: undefined }
    }
    this._qnext = null; // do not pull anymore
    this._resolveEnded();
  }
  async next() {
    if (!this._qpush) return { done: true, value: undefined };
    const unpushed = this._qpush.shift();
    if (unpushed) return unpushed.resolve(), unpushed;
    if (!this._qnext) return { done: true, value: this._value };
    return new Promise(this._pauseNextAsync);
  }
  async return(value) {
    if (this._qpush) {
      this._qpush.forEach(it => it.resolve());
      this._qpush = null; // do not push anymore
    }
    await this._whenEnded;
    return { done: true, value };
  }
  [Symbol.asyncIterator]() {
    return this;
  }
}

class SaslScramSha256 {
  _clientFirstMessageBare;
  _serverSignatureB64;
  async start() {
    const clientNonce = await _crypto.randomBytes(24);
    this._clientFirstMessageBare = 'n=,r=' + _crypto.b64encode(clientNonce);
    return 'n,,' + this._clientFirstMessageBare;
  }
  async continue(serverFirstMessage, password) {
    const utf8enc = new TextEncoder();
    const { 'i': iterations, 's': saltB64, 'r': nonceB64 } = this._parseMsg(serverFirstMessage);
    const finalMessageWithoutProof = 'c=biws,r=' + nonceB64;
    const salt = _crypto.b64decode(saltB64);
    const passwordUtf8 = utf8enc.encode(password.normalize());
    const saltedPassword = await _crypto.sha256pbkdf2(passwordUtf8, salt, +iterations, 32);
    const clientKey = await _crypto.sha256hmac(saltedPassword, utf8enc.encode('Client Key'));
    const storedKey = await _crypto.sha256(clientKey);
    const authMessage = utf8enc.encode(
      this._clientFirstMessageBare + ',' +
      serverFirstMessage + ',' +
      finalMessageWithoutProof
    );
    const clientSignature = await _crypto.sha256hmac(storedKey, authMessage);
    const clientProof = xor(clientKey, clientSignature);
    const serverKey = await _crypto.sha256hmac(saltedPassword, utf8enc.encode('Server Key'));
    this._serverSignatureB64 = _crypto.b64encode(await _crypto.sha256hmac(serverKey, authMessage));
    return finalMessageWithoutProof + ',p=' + _crypto.b64encode(clientProof);

    function xor(a, b) {
      return Uint8Array.from(a, (ai, i) => ai ^ b[i]);
    }
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
}

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

export const _net = {
  async connect(options) {
    return Deno.connect(options);
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
      if (err instanceof Deno.errors.BadResource) return; // already closed
      throw err;
    }
  },
};

// for scram-sha-256
export const _crypto = {
  b64encode(bytes) {
    return btoa(String.fromCharCode(...bytes));
  },
  b64decode(b64) {
    return Uint8Array.from(atob(b64), x => x.charCodeAt());
  },
  async randomBytes(n) {
    return crypto.getRandomValues(new Uint8Array(n));
  },
  async sha256(val) {
    return new Uint8Array(await crypto.subtle.digest('SHA-256', val));
  },
  async sha256hmac(key, inp) {
    const hmacParams = { name: 'HMAC', hash: 'SHA-256' };
    const importedKey = await crypto.subtle.importKey('raw', key, hmacParams, false, ['sign']);
    const buf = await crypto.subtle.sign('HMAC', importedKey, inp);
    return new Uint8Array(buf);
  },
  async sha256pbkdf2(pwd, salt, iterations, nbytes) {
    const cryptoKey = await crypto.subtle.importKey('raw', pwd, 'PBKDF2', false, ['deriveBits']);
    const pbkdf2params = { name: 'PBKDF2', hash: 'SHA-256', salt, iterations };
    const buf = await crypto.subtle.deriveBits(pbkdf2params, cryptoKey, nbytes * 8);
    return new Uint8Array(buf);
  },
};
