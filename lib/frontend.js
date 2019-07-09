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
    w.writeInt32BE(counter.length + 4);
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
    w.writeInt32BE(0x00030000);
    for (const [key, val] of Object.entries(this._options)) {
      w.writeCStr(key);
      w.writeCStr(String(val));
    }
    w.writeUInt8(0);
  }
}

class PasswordMessage {
  constructor(pwd) {
    this.tag = 'p';
    this._pwd = pwd;
  }
  writePayload(w) {
    w.writeCStr(this._pwd);
  }
}

class SASLInitialResponse {
  constructor({ mechanism, data }) {
    this.tag = 'p';
    this._mechanism = mechanism;
    this._data = data;
  }
  writePayload(w) {
    w.writeCStr(this._mechanism);
    if (this._data) {
      w.writeInt32BE(Buffer.byteLength(this._data));
      w.write(this._data);
    } else {
      w.writeInt32BE(-1);
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
    w.writeCStr(this._sql);
  }
}

class Parse {
  constructor(options) {
    this.tag = 'P';
    if (typeof options == 'string') {
      options = { statement: options };
    }
    const { statement, statementName = '', paramTypes = [] } = options;
    this._statementName = statementName;
    this._statement = statement;
    this._paramTypes = paramTypes;
  }
  writePayload(w) {
    w.writeCStr(this._statementName);
    w.writeCStr(this._statement);
    w.writeInt16BE(this._paramTypes.length);
    for (const typid of this._paramTypes) {
      w.writeUInt32BE(typid || 0);
    }
  }
}

class Bind {
  constructor({ portal = '', statementName = '', params = [], outFormats0t1b = [] } = {}) {
    this.tag = 'B';
    this._portal = portal;
    this._statementName = statementName;
    this._params = params;
    this._outFormats0t1b = outFormats0t1b;
  }
  writePayload(w) {
    w.writeCStr(this._portal);
    w.writeCStr(this._statementName);
    w.writeInt16BE(this._params.length);
    for (const p of this._params) {
      w.writeInt16BE(Buffer.isBuffer(p) ? 1 : 0);
    }
    w.writeInt16BE(this._params.length);
    for (const p of this._params) {
      if (p == null) {
        w.writeInt32BE(-1);
      } else {
        w.writeInt32BE(Buffer.byteLength(p));
        w.write(p);
      }
    }
    w.writeInt16BE(this._outFormats0t1b.length);
    for (const fmt of this._outFormats0t1b) {
      w.writeInt16BE(fmt);
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
    w.writeCStr(this._portal);
    w.writeUInt32BE(this._limit);
  }
}

class DescribeStatement {
  constructor(statementName = '') {
    this.tag = 'D';
    this._statementName = statementName;
  }
  writePayload(w) {
    w.writeUInt8('S'.charCodeAt());
    w.writeCStr(this._statementName);
  }
}

class DescribePortal {
  constructor(portal = '') {
    this.tag = 'D';
    this._portal = portal;
  }
  writePayload(w) {
    w.writeUInt8('P'.charCodeAt());
    w.writeCStr(this._portal);
  }
}

class ClosePortal {
  constructor(portal = '') {
    this.tag = 'C';
    this._portal = portal;
  }
  writePayload(w) {
    w.writeUInt8('P'.charCodeAt());
    w.writeCStr(this._portal);
  }
}

class CloseStatement {
  constructor(statementName = '') {
    this.tag = 'C';
    this._statementName = statementName;
  }
  writePayload(w) {
    w.writeUInt8('S'.charCodeAt());
    w.writeCStr(this._statementName);
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
    w.writeCStr(this._cause);
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
  writeUInt8() {
    this.length++;
  }
  writeInt16BE() {
    this.length += 2;
  }
  writeInt32BE() {
    this.length += 4;
  }
  writeUInt32BE() {
    this.length += 4;
  }
  writeCStr(s) {
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
  writeUInt8(b) {
    this._pos = this._buf.writeUInt8(b, this._pos);
  }
  writeInt16BE(i) {
    this._pos = this._buf.writeInt16BE(i, this._pos);
  }
  writeInt32BE(i) {
    this._pos = this._buf.writeInt32BE(i, this._pos);
  }
  writeUInt32BE(i) {
    this._pos = this._buf.writeUInt32BE(i, this._pos);
  }
  writeCStr(s) {
    this._pos += this._buf.write(s, this._pos);
    this._pos = this._buf.writeUInt8(0, this._pos);
  }
  write(inp) {
    if (Buffer.isBuffer(inp)) {
      this._pos += inp.copy(this._buf, this._pos);
    } else {
      this._pos += this._buf.write(inp, this._pos);
    }
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
