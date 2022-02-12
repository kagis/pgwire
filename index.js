import net from 'net';
import tls from 'tls';
import { createHash, createHmac, pbkdf2 as _pbkdf2, randomFill as _randomFill } from 'crypto';
import { once } from 'events';
import { promisify } from 'util';
import { _net, _crypto } from './mod.js';
export * from './mod.js';

const randomFill = promisify(_randomFill);
const pbkdf2 = promisify(_pbkdf2);

Object.assign(_net, {
  async connect({ hostname, port }) {
    const socket = SocketAdapter.attach(net.connect({ host: hostname, port }));
    await once(socket, 'connect');
    return socket;
  },
  reconnectable(err) {
    return err && (
      err.code == 'ENOTFOUND' ||
      err.code == 'ECONNREFUSED' ||
      err.code == 'ECONNRESET'
    );
  },
  async startTls(socket, { hostname, caCerts }) {
    // https://nodejs.org/docs/latest-v14.x/api/tls.html#tls_tls_connect_options_callback
    const tlssock = SocketAdapter.attach(tls.connect({
      secureContext: tls.createSecureContext({ ca: caCerts }),
      host: hostname,
      socket,
    }));
    await once(tlssock, 'secureConnect');
    tlssock.on('close', _ => socket.destroy());
    return tlssock;
  },
  async read(socket, out) {
    // return sockadapt.read(buf);
    return SocketAdapter.get(socket).read(out);
  },
  async write(socket, data) {
    return SocketAdapter.get(socket).write(data);
  },
  closeNullable(socket) {
    if (!socket) return;
    return SocketAdapter.get(socket).close();
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
  static kAdapter = Symbol('SocketAdapter');
  static attach(socket) {
    socket[this.kAdapter] = new this(socket);
    return socket;
  }
  static get(socket) {
    return socket[this.kAdapter];
  }
  constructor(socket) {
    this._socket = socket;
    this._readResume = Boolean;
    this._writeResume = Boolean;
    this._readPauseAsync = resolve => this._readResume = resolve;
    this._writePauseAsync = resolve => this._writeResume = resolve;
    this._socket.on('readable', _ => this._readResume());
    this._socket.on('end', _ => this._readResume());
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
    const p = new Promise(this._writePauseAsync);
    this._socket.write(data, this._writeResume);
    await p;
    if (this._error) throw this._error; // TODO callstack
    return data.length;
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
