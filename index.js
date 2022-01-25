import { connect } from 'net';
import { createHash, createHmac, pbkdf2 as _pbkdf2, randomFill as _randomFill } from 'crypto';
import { once } from 'events';
import { promisify } from 'util';
import { _net, _crypto } from './mod.js';
export * from './mod.js';

const randomFill = promisify(_randomFill);
const pbkdf2 = promisify(_pbkdf2);

Object.assign(_net, {
  async connect(options) {
    return SocketAdapter.connect(options);
  },
  async read(conn, buf) {
    return conn.read(buf);
  },
  async write(conn, data) {
    return conn.write(data);
  },
  close(conn) {
    return conn.close();
  },
});

// for scram-sha-256
Object.assign(_crypto, {
  b64encode(bytes) {
    return Buffer.from(bytes).toString('base64');
  },
  b64decode(b64) {
    return Uint8Array.from(Buffer.from(b64, 'base64'));
  },
  async randomBytes(n) {
    const buf = new Uint8Array(n);
    await randomFill(buf);
    return buf;
  },
  async sha256(val) {
    return Uint8Array.from(createHash('sha256').update(val).digest());
  },
  async sha256hmac(key, inp) {
    return Uint8Array.from(createHmac('sha256', key).update(inp).digest());
  },
  async sha256pbkdf2(pwd, salt, iterations, nbytes) {
    const buf = await pbkdf2(pwd, salt, iterations, nbytes, 'sha256');
    return Uint8Array.from(buf);
  },
});

class SocketAdapter {
  static async connect({ hostname, port }) {
    const socket = connect({ host: hostname, port });
    await once(socket, 'connect');
    return new this(socket);
  }
  constructor(socket) {
    this._socket = socket;
    this._readResume = Boolean;
    this._writeResume = Boolean;
    this._readPauseAsync = resolve => this._readResume = resolve;
    this._writePauseAsync = resolve => this._writeResume = resolve;
    this._socket.on('readable', _ => this._readResume());
    this._socket.on('end', _ => this._readResume());
    this._socket.on('drain', _ => this._writeResume());
    this._socket.on('error', error => {
      this._error = error;
      this._readResume();
      this._writeResume();
    });
  }

  /** @param {Uint8Array} out */
  async read(out) {
    let buf;
    for (;;) {
      if (this._error) throw this._error; // TODO callstack
      if (this._socket.readableEnded) return null;
      buf = this._socket.read();
      if (buf) break;
      await new Promise(this._readPauseAsync);
    }
    if (buf.length > out.length) {
      out.set(buf.subarray(0, out.length));
      this._socket.unshift(buf.subarray(out.length));
      return out.length;
    }
    out.set(buf);
    return buf.length;
  }
  async write(data) {
    // TODO assert Uint8Array
    // TODO need to copy data?
    if (this._error) throw this._error; // TODO callstack
    if (this._socket.write(data)) return data.length;
    await new Promise(this._writePauseAsync);
    if (this._error) throw this._error; // TODO callstack
  }
  // async closeWrite() {
  //   if (this._error) throw this._error; // TODO callstack
  //   const socket_end = promisify(cb => this._socket.end(cb));
  //   await socket_end();
  // }
  close() {
    this._socket.destroy();
  }
}
