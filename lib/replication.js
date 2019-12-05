const { Readable, Transform, PassThrough, pipeline, finished } = require('stream');
const BufferReader = require('./bufreader.js');
const { pgliteral, pgident } = require('./escape.js');
const { datatypesById } = require('./datatypes.js');

module.exports = {
  logicalReplication,
};

async function logicalReplication(conn, {
  slot,
  startLsn = '0/0',
  ackIntervalMillis,
  options = {},
}) {
  if (!/^[\da-f]{1,8}\/[\da-f]{1,8}$/i.test(startLsn)) {
    throw Object.assign(TypeError(), {
      code: 'PGERR_INVALID_START_LSN',
      message: 'Invalid streaming start location',
    });
  }
  if (ackIntervalMillis == null) {
    const { scalar } = await conn.query(/*sql*/ `
      SELECT setting::int
      FROM pg_catalog.pg_settings
      WHERE name = 'wal_sender_timeout'
    `);
    ackIntervalMillis = scalar > 0 ? scalar / 2 : 60e3;
  } else if (!(Number.isFinite(ackIntervalMillis) && ackIntervalMillis > 0)) {
    throw Object.assign(TypeError(), {
      code: 'PGERR_INVALID_ACK_INTERVAL',
    });
  }
  const optionsSql = (
    Object.entries(options)
    // FIXME option key is injectable
    .map(([k, v]) => k + ' ' + pgliteral(v))
    .join(',')
    .replace(/.+/, '($&)')
  );
  const tx = new PassThrough();
  const rx = conn.query(
    `START_REPLICATION SLOT ${pgident(slot)} LOGICAL ${startLsn} ${optionsSql}`,
    { stdin: tx },
  );
  // TODO https://nodejs.org/api/events.html#events_events_once_emitter_name
  await new Promise((resolve, reject) => {
    rx.on('error', reject);
    rx.on('readable', function onReadable() {
      rx.off('readable', onReadable);
      rx.off('error', reject);
      return resolve();
    });
  });
  return new ReplicationStream({ rx, tx, startLsn, ackIntervalMillis });
}

class ReplicationStream extends Readable {
  constructor({ rx, tx, startLsn, ackIntervalMillis }) {
    super({ objectMode: true });
    this._tx = tx;
    this._rx = rx;
    this._ackingLsn = startLsn;
    this._ackTimer = setTimeout(this.ackImmediate.bind(this), ackIntervalMillis);
    finished(this._tx, this._onRxTxFinished.bind(this));
    finished(this._rx, this._onRxTxFinished.bind(this));
    this._rx.on('data', this._onData.bind(this));
    this._rx.on('end', _ => this.push(null));
  }
  pgoutput() {
    return pipeline(this, new PgoutputDecoder(), _ => _);
  }
  ack(lsn) {
    if (!this._tx) {
      throw Object.assign(Error(), {
        name: 'PGError',
        code: 'PGERR_ACK_AFTER_REPLICATION_END',
        message: 'Cannot ack after replication end',
      });
    }
    if (lsn > this._ackingLsn) {
      this._ackingLsn = lsn;
    }
  }
  ackImmediate(lsn) {
    this.ack(lsn);
    this._ackTimer.refresh();
    if (this._tx.writableHighWaterMark > this._tx.writableLength) {
      return this._tx.write(updateStatusMessage(incLsn(this._ackingLsn)));
    }
  }
  _onRxTxFinished(err) {
    this._tx = null;
    this.destroy(err);
  }
  _destroy(err, callback) {
    clearTimeout(this._ackTimer);
    if (this._tx) {
      this._tx.end(updateStatusMessage(incLsn(this._ackingLsn)));
      this._tx = null;
      finished(this._rx, _ => super._destroy(err, callback));
      this._rx.resume();
    } else {
      super._destroy(err, callback);
    }
  }
  _onData(chunk) {
    if (chunk.boundary || this.destroyed) {
      return;
    }
    const replmsg = readReplicationMessage(chunk);
    switch (replmsg.tag) {
      case 'XLogData':
        if (!this.push(replmsg)) {
          this._rx.pause();
        }
        break;
      case 'PrimaryKeepaliveMessage': {
        if (replmsg.shouldReply) {
          this.ackImmediate();
        }
        break;
      }
    }
  }
  _read() {
    this._rx.resume();
  }
}

function readReplicationMessage(buf) {
  const bufr = new BufferReader(buf);
  switch (bufr.readUInt8()) {
    case 0x77 /*w*/: return {
      tag: 'XLogData',
      lsn: readLsn(bufr),
      endLsn: readLsn(bufr),
      time: readTime(bufr),
      data: bufr.readToEnd(),
    };
    case 0x6b /*k*/: return {
      tag: 'PrimaryKeepaliveMessage',
      endLsn: readLsn(bufr),
      time: readTime(bufr),
      shouldReply: bufr.readUInt8(),
    };
    default: throw Error('Unknown replication message');
  }
}

class PgoutputDecoder extends Transform {
  constructor() {
    super({ objectMode: true });
    this._relations = {};
    this._attrs = {};
  }
  _transform(xlogmsg, _enc, done) {
    const bufr = new BufferReader(xlogmsg.data);
    const tag = bufr.readUInt8();
    switch (tag) {
      case 0x52 /*R*/: return done(null, this._handleRelation(xlogmsg, bufr));
      case 0x59 /*Y*/: return done(null, this._handleType(xlogmsg, bufr));
      case 0x4f /*O*/: return done(null, this._handleOrigin(xlogmsg, bufr));
      case 0x42 /*B*/: return done(null, this._handleBegin(xlogmsg, bufr));
      case 0x49 /*I*/: return done(null, this._handleInsert(xlogmsg, bufr));
      case 0x44 /*D*/: return done(null, this._handleDelete(xlogmsg, bufr));
      case 0x55 /*U*/: return done(null, this._handleUpdate(xlogmsg, bufr));
      case 0x54 /*T*/: return done(null, this._handleTruncate(xlogmsg, bufr));
      case 0x43 /*C*/: return done(null, this._handleCommit(xlogmsg, bufr));
      default: return done(null, xlogmsg);
    }
  }
  _handleRelation({ lsn, endLsn, time }, bufr) {
    const rel = {
      relationid: bufr.readInt32BE(),
      schema: bufr.readCStr(),
      name: bufr.readCStr(),
      replicaIdentity: String.fromCharCode(bufr.readUInt8()),
      attrs: Array(bufr.readInt16BE()).fill(null).map(_ => ({
        flags: bufr.readUInt8(),
        name: bufr.readCStr(),
        typeid: bufr.readInt32BE(),
        typemod: bufr.readInt32BE(),
      })),
    };
    this._relations[rel.relationid] = rel;
    this._attrs[rel.relationid] = rel.attrs.map(att => {
      const { decodeText = noop => noop } = datatypesById[att.typeid] || {};
      return { ...att, decode: decodeText };
    });
    return {
      tag: 'relation',
      lsn,
      endLsn,
      time,
      ...rel,
    };
  }
  _handleType(_, bufr) {
    return {
      tag: 'type',
      typeid: bufr.readInt32BE(),
      schema: bufr.readCStr(),
      name: bufr.readCStr(),
    };
  }
  _handleOrigin(_, bufr) {
    return {
      tag: 'origin',
      lsn: readLsn(bufr),
      origin: bufr.readCStr(),
    };
  }
  _handleBegin({ lsn, endLsn, time }, bufr) {
    return {
      tag: 'begin',
      lsn,
      endLsn,
      time,
      finalLsn: readLsn(bufr),
      commitTime: readTime(bufr),
      xid: bufr.readInt32BE(),
    };
  }
  _handleCommit({ lsn, endLsn, time }, bufr) {
    return {
      tag: 'commit',
      lsn,
      endLsn,
      time,
      flags: bufr.readUInt8(),
      commitLsn: readLsn(bufr),
      _endLsn: readLsn(bufr),
      commitTime: readTime(bufr),
    };
  }
  _handleInsert({ lsn, endLsn, time }, bufr) {
    const relid = bufr.readInt32BE();
    const _kind = bufr.readUInt8();
    const after = readTuple(bufr, this._attrs[relid]);
    return {
      tag: 'insert',
      lsn,
      endLsn,
      time,
      relation: this._relations[relid],
      after,
    };
  }
  _handleUpdate({ lsn, endLsn, time }, bufr) {
    const relid = bufr.readInt32BE();
    const attrs = this._attrs[relid];
    let kind = bufr.readUInt8();
    let before = null;
    if (kind == 0x4b /*K*/ || kind == 0x4f /*O*/) {
      before = readTuple(bufr, attrs);
      kind = bufr.readUInt8();
    }
    const after = readTuple(bufr, attrs);
    return {
      tag: 'update',
      lsn,
      endLsn,
      time,
      relation: this._relations[relid],
      before,
      after,
    };
  }
  _handleDelete({ lsn, endLsn, time }, bufr) {
    const relid = bufr.readInt32BE();
    const kind = bufr.readUInt8();
    const before = readTuple(bufr, this._attrs[relid]);
    return {
      tag: 'delete',
      lsn,
      endLsn,
      time,
      relation: this._relations[relid],
      keyOnly: kind == 0x4b /*K*/,
      before,
    };
  }
  _handleTruncate({ lsn, endLsn, time }, bufr) {
    const nrels = bufr.readInt32BE();
    const flags = bufr.readUInt8();
    return {
      tag: 'truncate',
      lsn,
      endLsn,
      time,
      cascade: Boolean(flags & 0b1),
      restartSeqs: Boolean(flags & 0b10),
      relations: (
        Array(nrels)
        .fill(0)
        .map(_ => bufr.readInt32BE())
        .map(relid => this._relations[relid])
      ),
    };
  }
}

function readTuple(bufr, attrs) {
  const n = bufr.readInt16BE();
  const tuple = {}; // Object.create(null);
  for (let i = 0; i < n; i++) {
    const { name, decode } = attrs[i];
    switch (bufr.readUInt8()) {
      case 0x74 /*t*/:
        tuple[name] = decode(readCountedStr(bufr));
        break;
      case 0x6e /*n*/:
        tuple[name] = null;
        break;
      default: // 'u'
        tuple[name] = undefined;
    }
  }
  return tuple;
}

function updateStatusMessage(lsn) {
  const [a, b] = lsn.split('/');
  const lsnbuf = Buffer.from(a.padStart(8, '0') + b.padStart(8, '0'), 'hex');
  const msg = Buffer.allocUnsafe(1 + 8 + 8 + 8 + 8 + 1);
  msg.fill(0);
  msg[0] = 0x72/*r*/;
  lsnbuf.copy(msg, 1);
  lsnbuf.copy(msg, 1 + 8);
  lsnbuf.copy(msg, 1 + 8 + 8);
  return msg;
}

function incLsn(lsn) {
  const [a, b] = lsn.split('/').map(x => parseInt(x, 16));
  if (b > 0xffffffff) {
    throw Error('invalid lsn');
  }
  const [ainc, binc] = b == 0xffffffff ? [a + 1, 0] : [a, b + 1];
  return (
    ainc.toString(16).padStart(8, '0') + '/' +
    binc.toString(16).padStart(8, '0')
  ).toUpperCase();
}

function readCountedStr(bufr) {
  return bufr.readExact(bufr.readInt32BE()).toString();
}

function readLsn(bufr) {
  // use zero-padded lsn format because it is comparable
  // by string comparer and is still valid pg_lsn representation
  return (
    bufr.readExact(4).toString('hex').toUpperCase() + '/' +
    bufr.readExact(4).toString('hex').toUpperCase()
  );
}

function readTime(bufr) {
  return bufr.readExact(8);
}
