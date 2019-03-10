const { Transform, finished } = require('stream');
const { debuglog } = require('util');

const logFeMsg = debuglog('pgwire-fe');

class FrontendEncoder extends Transform {
  constructor() {
    super({
      writableObjectMode: true,
      writableHighWaterMark: 1000,
    });
    this._copyUpstream = null;
  }
  _transform(message, _enc, done) {
    if (message instanceof SubProtocol) {
      return this._enterSubProtocol(message.stream, done);
    }
    try {
      this._pushMessage(message);
    } catch (err) {
      return done(err);
    }
    return done();
  }
  _read(n) {
    if (this._substream) {
      return this._substream.resume();
    }
    return super._read(n);
  }
  _pushMessage(message) {
    logFeMsg('<- %j', message);
    const counter = new CounterWriter();
    message.writePayload(counter);
    const buf = Buffer.allocUnsafe(counter.length + 4);
    const w = new BufWriter(buf);
    w.write_i32be(counter.length + 4);
    message.writePayload(w);
    if (message.tag) {
      this.push(message.tag);
    }
    this.push(buf);
  }
  _enterSubProtocol(substream, done) {
    this._substream = substream;
    substream.on('data', chunk => {
      if (!this.push(chunk)) {
        substream.pause();
      }
    });
    finished(substream, err => {
      this._substream = null;
      return done(err);
    });
  }
  _destroy(err, done) {
    if (this._substream) {
      this._substream.destroy(err);
      this._substream = null;
    }
    return done();
  }
}

class StartupMessage {
  constructor(options) {
    this._options = options;
  }
  writePayload(w) {
    w.write_i32be(0x00030000);
    for (const [key, val] of Object.entries(this._options)) {
      w.write_cstr(key);
      w.write_cstr(String(val));
    }
    w.write_u8(0);
  }
}

class PasswordMessage {
  constructor(pwd) {
    this.tag = 'p';
    this._pwd = pwd;
  }
  writePayload(w) {
    w.write_cstr(this._pwd);
  }
}

class SASLInitialResponse {
  constructor({ mechanism, data }) {
    this.tag = 'p';
    this._mechanism = mechanism;
    this._data = data;
  }
  writePayload(w) {
    w.write_cstr(this._mechanism);
    if (this._data) {
      w.write_i32be(Buffer.byteLength(this._data));
      w.write(this._data);
    } else {
      w.write_i32be(-1);
    }
  }
}

class SASLResponse {
  constructor(data) {
    this.tag = 'p';
    this._data = data;
  }
  writePayload(w) {
    w.write(this._data);
  }
}

class Query {
  constructor(sql) {
    this.tag = 'Q';
    this._sql = sql;
  }
  writePayload(w) {
    w.write_cstr(this._sql);
  }
}

class Parse {
  constructor(sqlOrOptions) {
    this.tag = 'P';
    if (typeof sqlOrOptions == 'string') {
      sqlOrOptions = { sql: sqlOrOptions };
    }
    const { sql, name = '', paramTypes = [] } = sqlOrOptions;
    this._name = name;
    this._sql = sql;
    this._paramTypes = paramTypes;
  }
  writePayload(w) {
    w.write_cstr(this._name);
    w.write_cstr(this._sql);
    w.write_i16be(this._paramTypes.length);
    for (const typ_oid of this._paramTypes) {
      w.write_u32be(typ_oid || 0);
    }
  }
}

class Bind {
  constructor({ portal = '', name = '', params = [], outFormats0t1b = [] } = {}) {
    this.tag = 'B';
    this._portal = portal;
    this._name = name;
    this._params = params;
    this._outFormats0t1b = outFormats0t1b;
  }
  writePayload(w) {
    w.write_cstr(this._portal);
    w.write_cstr(this._name);
    w.write_i16be(this._params.length);
    for (const p of this._params) {
      w.write_i16be(Buffer.isBuffer(p) ? 1 : 0);
    }
    w.write_i16be(this._params.length);
    for (const p of this._params) {
      if (p == null) {
        w.write_i32be(-1);
      } else {
        const p_buf = Buffer.isBuffer(p) ? p : Buffer.from(String(p));
        w.write_i32be(p_buf.length);
        w.write(p_buf);
      }
    }
    w.write_i16be(this._outFormats0t1b.length);
    for (const fmt of this._outFormats0t1b) {
      w.write_i16be(fmt);
    }
  }
}

class Execute {
  constructor({ portal = '', limit = 0 } = {}) {
    this.tag = 'E';
    this._portal = portal;
    this._limit = limit;
  }
  writePayload(w) {
    w.write_cstr(this._portal);
    w.write_u32be(this._limit);
  }
}

class DescribeStatement {
  constructor(statementName = '') {
    this.tag = 'D';
    this._statementName = statementName;
  }
  writePayload(w) {
    w.write_u8('S'.charCodeAt());
    w.write_cstr(this._statementName);
  }
}

class DescribePortal {
  constructor(portalName = '') {
    this.tag = 'D';
    this._portalName = portalName;
  }
  writePayload(w) {
    w.write_u8('P'.charCodeAt());
    w.write_cstr(this._portalName);
  }
}

class ClosePortal {
  constructor(portalName = '') {
    this.tag = 'C';
    this._portalName = portalName;
  }
  writePayload(w) {
    w.write_u8('P'.charCodeAt());
    w.write_cstr(this._portalName);
  }
}

class CloseStatement {
  constructor(statementName = '') {
    this.tag = 'C';
    this._statementName = statementName;
  }
  writePayload(w) {
    w.write_u8('S'.charCodeAt());
    w.write_cstr(this._statementName);
  }
}

class Sync {
  constructor() {
    this.tag = 'S';
  }
  writePayload() {}
}

class Flush {
  constructor() {
    this.tag = 'H';
  }
  writePayload() {}
}

class CopyData {
  constructor(data) {
    this.tag = 'd';
    this._data = data;
  }
  writePayload(w) {
    w.write(this._data);
  }
}

class CopyDone {
  constructor() {
    this.tag = 'c';
  }
  writePayload() {}
}

class CopyFail {
  constructor(cause) {
    this.tag = 'f';
    this._cause = cause;
  }
  writePayload(w) {
    w.write_cstr(this._cause);
  }
}

class Terminate {
  constructor() {
    this.tag = 'X';
  }
  writePayload() {}
}

class SubProtocol {
  constructor(stream) {
    this.stream = stream;
  }
}

class CounterWriter {
  constructor() {
    this.length = 0;
  }
  write_u8() {
    this.length++;
  }
  write_i16be() {
    this.length += 2;
  }
  write_i32be() {
    this.length += 4;
  }
  write_u32be() {
    this.length += 4;
  }
  write_cstr(s) {
    this.length += Buffer.byteLength(s) + 1;
  }
  write(buf) {
    this.length += Buffer.byteLength(buf);
  }
}

class BufWriter {
  constructor(buf) {
    this._buf = buf;
    this._pos = 0;
  }
  write_u8(b) {
    this._pos = this._buf.writeUInt8(b, this._pos);
  }
  write_i16be(i) {
    this._pos = this._buf.writeInt16BE(i, this._pos);
  }
  write_i32be(i) {
    this._pos = this._buf.writeInt32BE(i, this._pos);
  }
  write_u32be(i) {
    this._pos = this._buf.writeUInt32BE(i, this._pos);
  }
  write_cstr(s) {
    this._pos += this._buf.write(s, this._pos);
    this._pos = this._buf.writeUInt8(0, this._pos);
  }
  write(inp) {
    // FIXME Buffer.from copies buffer
    this._pos += Buffer.from(inp).copy(this._buf, this._pos);
  }
}

module.exports = {
  FrontendEncoder,
  StartupMessage,
  PasswordMessage,
  SASLInitialResponse,
  SASLResponse,
  Query,
  Parse,
  Bind,
  Execute,
  DescribeStatement,
  DescribePortal,
  CloseStatement,
  ClosePortal,
  CopyData,
  CopyDone,
  CopyFail,
  Flush,
  Sync,
  Terminate,
  SubProtocol,
};
