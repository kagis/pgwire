class BufferReader {
  constructor(buf) {
    this._buf = buf;
    this._pos = 0;
  }
  read_u8() {
    return this._buf.readInt8(this._move(1));
  }
  read_char() {
    return String.fromCharCode(this.read_u8());
  }
  read_i16be() {
    return this._buf.readInt16BE(this._move(2));
  }
  read_i32be() {
    return this._buf.readInt32BE(this._move(4));
  }
  read_u64be() {
    return this.read_exact(8);
  }
  read_cstr() {
    const end_idx = this._buf.indexOf(0, this._pos);
    if (end_idx < 0) {
      throw new Error('Unexpected EOF');
    }
    const start_idx = this._pos;
    this._pos = end_idx + 1;
    return this._buf.slice(start_idx, end_idx).toString();
  }
  read_countedstr() {
    const buf = this.read_exact(this.read_i32be());
    return buf.toString();
  }
  read_exact(len) {
    return this._buf.slice(this._move(len), this._pos);
  }
  read_to_end() {
    const pos = this._pos;
    this._pos = this._buf.length;
    return this._buf.slice(pos);
  }
  _move(len) {
    if (this._pos + len > this._buf.length) {
      throw new Error('Unexpected EOF');
    }
    const pos = this._pos;
    this._pos += len;
    return pos;
  }
}

module.exports = BufferReader;
