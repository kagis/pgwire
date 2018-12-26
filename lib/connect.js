const net = require('net');
const EventEmmiter = require('events');
const { Readable, finished } = require('stream');
const { PGBackendMsgStream } = require('./backend.js');
const fe = require('./frontend.js');

exports.JSONB_TYPE_OID = 3802;

exports.pgconnect = async function pgconnect({
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
    this._resp_streams = [];
  }

  async _connect({ hostname, port, password: _password, ...options }) {
    this._be_msg_stream = new PGBackendMsgStream();
    this._be_msg_stream.on('data', this._handle_message.bind(this));
    this._socket = net.connect({ host: hostname, port });
    this._socket.on('error', this._on_error.bind(this));
    finished(this._socket, _ => {
      this.emit('terminate');
    });
    this._socket.pipe(this._be_msg_stream);
    this._write_command(new fe.StartupMessage(options));
    for await (const _msg of this._cursor()) {
      // console.log('connecting', msg);
    }
    return this;
  }

  _on_error(err) {
    if (this._current_cursor) {
      return this._current_cursor.emit('error', err);
    }
    this.emit('error', err);
  }

  _write_command(msg) {
    return fe.write_message(this._socket, msg);
  }

  _handle_message(msg) {
    if (msg.tag == 'ReadyForQuery') {
      this._current_cursor.push(null);
      this._current_cursor = this._resp_streams.shift();
      if (this._current_cursor) {
        this._current_cursor.on('readrequest', _ => this._socket.resume());
      }
      return;
    }
    if (msg.tag == 'Notify') {
      return this.emit('notify', msg.payload);
    }
    if (msg.tag == 'ErrorResponse') {
      const { tag: _, code, ...err_props } = msg;
      const err = new Error();
      err.name = 'PGError';
      err.code = 'PGERR_' + code;
      Object.assign(err, err_props);
      return this._on_error(err);
    }
    if (msg.tag == 'NoticeResponse') {
      return console.log(msg);
    }
    if (!this._current_cursor.push(msg)) {
      this._socket.pause();
    }
  }

  parse(statement_or_options) {
    if (typeof statement_or_options == 'string') {
      statement_or_options = {
        statement: statement_or_options,
      };
    }
    this._write_command(new fe.Parse(statement_or_options));
    return this;
  }

  bind(opts = {}) {
    this._write_command(new fe.Bind(opts));
    return this;
  }

  execute(opts = {}) {
    this._write_command(new fe.Execute(opts));
    return this;
  }

  sync() {
    this._write_command(new fe.Sync());
    return this._cursor();
  }

  query(q) {
    this._write_command(new fe.Query(q));
    return this._cursor();
  }

  copydata(data) {
    this._write_command(new fe.CopyData(data));
    return this;
  }

  terminate() {
    this._write_command(new fe.Terminate());
    this._socket.end();
  }

  _cursor() {
    const cursor = new PGCursor();
    if (this._current_cursor) {
      this._resp_streams.push(cursor);
    } else {
      cursor.on('readrequest', _ => this._socket.resume());
      this._current_cursor = cursor;
    }
    return cursor;
  }
}

class PGCursor extends Readable {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 1000,
    });
  }
  _read() {
    this.emit('readrequest');
  }
  async fetch() {
    return new Promise((resolve, reject) => {
      let rows = [];
      const result = [];
      this.on('error', reject);
      this.on('data', msg => {
        switch (msg.tag) {
          case 'DataRow':
            return rows.push(msg.data);
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
      this.on('end', _ => resolve(result));
    });
  }
}
