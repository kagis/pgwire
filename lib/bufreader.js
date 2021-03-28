module.exports =
class BufferReader {
  constructor(buf) {
    this._buf = buf;
    this._pos = 0;
  }
  readUInt8() {
    return this._buf.readUInt8(this._move(1));
  }
  readInt16BE() {
    return this._buf.readInt16BE(this._move(2));
  }
  readInt32BE() {
    return this._buf.readInt32BE(this._move(4));
  }
  readUInt32BE() {
    return this._buf.readUInt32BE(this._move(4));
  }
  readCStr() {
    const endIdx = this._buf.indexOf(0, this._pos);
    if (endIdx < 0) {
      throw Error('Unexpected EOF');
    }
    const startIdx = this._pos;
    this._pos = endIdx + 1;
    return this._buf.slice(startIdx, endIdx).toString();
  }
  readExact(len) {
    return this._buf.slice(this._move(len), this._pos);
  }
  readToEnd() {
    const pos = this._pos;
    this._pos = this._buf.length;
    return this._buf.slice(pos);
  }
  _move(len) {
    if (this._pos + len > this._buf.length) {
      throw Error('Unexpected EOF');
    }
    const pos = this._pos;
    this._pos += len;
    return pos;
  }
};
