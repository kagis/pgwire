const { Transform, finished } = require('stream');

class FrontendEncoder extends Transform {
  constructor() {
    super({
      writableObjectMode: true,
      writableHighWaterMark: 1000,
    });
    this._copyUpstream = null;
  }
  _transform(message, _enc, done) {
    if (message instanceof _CopyUpstream) {
      return this._pushCopyUpstream(message.stream, done);
    }
    if (message instanceof _Block) {
      return message.promise.then(_ => done());
    }
    this._pushMessage(message);
    return done();
  }
  _read(n) {
    if (this._copyUpstream) {
      return this._copyUpstream.resume();
    }
    return super._read(n);
  }
  _pushMessage(message) {
    // console.error('<-', JSON.stringify(message));
    const counter = new CounterWriter();
    message.writePayload(counter);
    const buf = Buffer.allocUnsafe(counter.length + 4);
    const w = new BufWriter(buf);
    w.write_i32be(counter.length + 4);
    message.writePayload(w);
    if (message.ident) {
      this.push(message.ident);
    }
    this.push(buf);
  }
  _pushCopyUpstream(copyUpstream, done) {
    this._copyUpstream = copyUpstream;
    this._copyUpstream.on('data', chunk => {
      const header = Buffer.allocUnsafe(1 + 4);
      header.writeUInt8(0x64); // 'd'
      header.writeInt32BE(4 + Buffer.byteLength(chunk), 1);
      if (!(this.push(header) && this.push(chunk))) {
        this._copyUpstream.pause();
      }
    });
    finished(this._copyUpstream, err => {
      if (err) {
        this._pushMessage(new CopyFail(String(err)));
      } else {
        this._pushMessage(new CopyDone());
      }
      // this._pushMessage(new Flush());
      this._copyUpstream = null;
      return done();
    });
  }
}

class FrontendMessage  {
  writePayload() {}
}

class StartupMessage extends FrontendMessage {
  constructor(options) {
    super();
    this._options = options;
  }
  writePayload(w) {
    w.write_i32be(0x00030000);
    for (const [key, val] of Object.entries(this._options)) {
      w.write_cstr(key);
      w.write_cstr(val);
    }
    w.write_u8(0);
  }
}

class PasswordMessage extends FrontendMessage {
  constructor(pwd) {
    super();
    this.ident = 'p';
    this._pwd = pwd;
  }
  writePayload(w) {
    w.write_cstr(this._pwd);
  }
}

class Query extends FrontendMessage  {
  constructor(sql) {
    super();
    this.ident = 'Q';
    this._sql = sql;
  }
  writePayload(w) {
    w.write_cstr(this._sql);
  }
}

class Parse extends FrontendMessage  {
  constructor(sqlOrOptions) {
    super();
    this.ident = 'P';
    if (typeof sqlOrOptions == 'string') {
      sqlOrOptions = { statement: sqlOrOptions };
    }
    const { statement, name = '',  paramtypes = [] } = sqlOrOptions;
    this._name = name;
    this._statement = statement;
    this._paramtypes = paramtypes;
  }
  writePayload(w) {
    w.write_cstr(this._name);
    w.write_cstr(this._statement);
    w.write_i16be(this._paramtypes.length);
    for (const typ_oid of this._paramtypes) {
      w.write_i32be(typ_oid || 0);
    }
  }
}

class Bind extends FrontendMessage  {
  constructor({ portal = '', name = '', params = [] } = {}) {
    super();
    this.ident = 'B';
    this._portal = portal;
    this._name = name;
    this._params = params;
  }
  writePayload(w) {
    w.write_cstr(this._portal);
    w.write_cstr(this._name);
    w.write_i16be(0); // text
    w.write_i16be(this._params.length);
    for (const p of this._params) {
      if (p == null) {
        w.write_i32be(-1);
        continue;
      }
      const p_buf = Buffer.from(String(p));
      w.write_i32be(p_buf.length);
      w.write(p_buf);
    }
    w.write_i16be(0); // text
    // w.write_i16be(1); // binary
  }
}

class Execute extends FrontendMessage  {
  constructor({ portal = '', limit = 0 } = {}) {
    super();
    this.ident = 'E';
    this._portal = portal;
    this._limit = limit;
  }
  writePayload(w) {
    w.write_cstr(this._portal);
    w.write_u32be(this._limit);
  }
}

class ClosePortal extends FrontendMessage  {
  constructor(portalName) {
    super();
    this.ident = 'C';
    this._portalName = portalName;
  }
  writePayload(w) {
    w.write_u8('P'.charCodeAt());
    w.write_cstr(this._portalName);
  }
}

class CloseStatement extends FrontendMessage  {
  constructor(statementName) {
    super();
    this.ident = 'C';
    this._statementName = statementName;
  }
  writePayload(w) {
    w.write_u8('S'.charCodeAt());
    w.write_cstr(this._statementName);
  }
}

class Sync extends FrontendMessage  {
  constructor() {
    super();
    this.ident = 'S';
  }
}

class Flush extends FrontendMessage  {
  constructor() {
    super();
    this.ident = 'H';
  }
}

class CopyData extends FrontendMessage {
  constructor(data) {
    super();
    this.ident = 'd';
    this._data = data;
  }
  writePayload(w) {
    w.write(this._data);
  }
};

class CopyDone extends FrontendMessage  {
  constructor() {
    super();
    this.ident = 'c';
  }
}

class CopyFail extends FrontendMessage  {
  constructor(cause) {
    super();
    this.ident = 'f';
    this._cause = cause;
  }
  writePayload(w) {
    w.write_cstr(this._cause);
  }
}

class Terminate extends FrontendMessage {
  constructor() {
    super();
    this.ident = 'X';
  }
}

class _CopyUpstream {
  constructor(stream) {
    this.stream = stream;
  }
}

class _Block {
  constructor(promise) {
    this.promise = promise;
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
    this._pos += Buffer.from(inp).copy(this._buf, this._pos);
  }
}

Object.assign(exports, {
  FrontendEncoder,
  StartupMessage,
  PasswordMessage,
  Query,
  Parse,
  Bind,
  Execute,
  CloseStatement,
  ClosePortal,
  CopyData,
  CopyDone,
  CopyFail,
  Flush,
  Sync,
  Terminate,
  _CopyUpstream,
  _Block,
});
