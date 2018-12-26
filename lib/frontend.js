exports.write_message = function write_message(target, message) {
  const counter = new CounterWriter();
  message.write_payload(counter);
  const w = new Writer(target);
  if (message.ident) {
    w.write_u8(message.ident);
  }
  w.write_i32be(counter.length + 4);
  message.write_payload(w);
}

exports.StartupMessage = class StartupMessage {
  constructor(options) {
    this._options = options;
  }
  write_payload(w) {
    w.write_i32be(0x00030000);
    for (const [key, val] of Object.entries(this._options)) {
      w.write_cstr(key);
      w.write_cstr(val);
    }
    w.write_u8(0);
  }
}

exports.PasswordMessage = class PasswordMessage {
  constructor(pwd) {
    this.ident = 'p';
    this._pwd = pwd;
  }
  write_payload(w) {
    w.write_cstr(this._pwd);
  }
}

exports.Query = class Query {
  constructor(query) {
    this.ident = 'Q';
    this._query = query;
  }
  write_payload(w) {
    w.write_cstr(this._query);
  }
}

exports.Parse = class Parse {
  constructor({ name = '', statement, paramtypes = [] }) {
    this.ident = 'P';
    this._name = name;
    this._statement = statement;
    this._paramtypes = paramtypes;
  }
  write_payload(w) {
    w.write_cstr(this._name);
    w.write_cstr(this._statement);
    w.write_i16be(this._paramtypes.length);
    for (const typ_oid of this._paramtypes) {
      w.write_i32be(typ_oid || 0);
    }
  }
}

exports.Bind = class Bind {
  constructor({ portal = '', name = '', params = [] }) {
    this.ident = 'B';
    this._portal = portal;
    this._name = name;
    this._params = params;
  }
  write_payload(w) {
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

exports.Execute = class Execute {
  constructor({ portal = '', limit = 0 }) {
    this.ident = 'E';
    this._portal = portal;
    this._limit = limit;
  }
  write_payload(w) {
    w.write_cstr(this._portal);
    w.write_u32be(this._limit);
  }
}

exports.Sync = class Sync {
  constructor() {
    this.ident = 'S';
  }
  write_payload() { }
}

// export class Flush {
//   constructor() {
//     this.ident = 'H';
//   }
//   write_payload() { }
// }

exports.CopyData = class CopyData {
  constructor(data) {
    this.ident = 'd';
    this._data = data;
  }
  write_payload(w) {
    w.write(this._data);
  }
}

exports.Terminate = class Terminate {
  constructor() {
    this.ident = 'X';
  }
  write_payload() {}
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
    this.length += buf.length;
  }
}

class Writer {
  constructor(stream) {
    this._stream = stream;
  }
  write_u8(b) {
    this._stream.write(Buffer.allocUnsafe(1).fill(b));
  }
  write_i16be(i) {
    const buf = Buffer.allocUnsafe(2);
    buf.writeInt16BE(i);
    this._stream.write(buf);
  }
  write_i32be(i) {
    const buf = Buffer.allocUnsafe(4);
    buf.writeInt32BE(i);
    this._stream.write(buf);
  }
  write_u32be(i) {
    const buf = Buffer.allocUnsafe(4);
    buf.writeUInt32BE(i);
    this._stream.write(buf);
  }
  write_cstr(s) {
    this._stream.write(s);
    this._stream.write('\0');
  }
  write(buf) {
    this._stream.write(buf);
  }
}
