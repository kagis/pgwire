const { Readable, Transform, PassThrough, pipeline } = require('stream');
const BufferReader = require('./bufreader.js');
const { pgliteral, pgident } = require('./escape.js');
const { datatypesById } = require('./datatypes.js');

module.exports = {
  logicalReplication,
};

async function logicalReplication(conn, {
  slot,
  startLsn, // todo validate lsn format
  ackIntervalMs,
  options = {},
}) {
  if (ackIntervalMs == null) {
    const { scalar } = await conn.query(/*sql*/ `
      SELECT setting::int
      FROM pg_catalog.pg_settings
      WHERE name = 'wal_sender_timeout'
    `);
    ackIntervalMs = scalar / 2;
  }
  const optionsSql = (
    Object.entries(options)
    .map(([k, v]) => k + ' ' + pgliteral(v))
    .join(',')
    .replace(/.+/, '($&)')
  );
  const tx = new PassThrough();
  const rx = conn.stream({
    script: `START_REPLICATION SLOT ${pgident(slot)} LOGICAL ${startLsn} ${optionsSql}`,
    stdin: tx,
  });
  // wait for CopyBothResponse
  await new Promise((resolve, reject) => {
    rx.on('error', reject);
    rx.on('data', function ondata({ tag }) {
      if (tag == 'CopyBothResponse') {
        rx.off('data', ondata);
        rx.off('error', reject);
        rx.pause();
        return resolve();
      }
    });
  });
  return new ReplicationStream({ rx, tx, startLsn, ackIntervalMs });
}

class ReplicationStream extends Readable {
  constructor({ rx, tx, startLsn, ackIntervalMs }) {
    super({ objectMode: true });
    this._tx = tx;
    this._rx = rx;
    this._ackingLsn = startLsn;
    this._ackTimer = setTimeout(this.ackImmediate.bind(this), ackIntervalMs);
    this._tx.on('finish', _ => clearTimeout(this._ackTimer));
    this._rx.on('data', this._onData.bind(this));
    this._rx.on('end', _ => this.push(null));
    this._rx.on('error', err => this.emit('error', err));
  }
  pgoutput() {
    return pipeline(this, new PgoutputDecoder(), _ => _);
  }
  ack(lsn) {
    if (lsn > this._ackingLsn) {
      this._ackingLsn = lsn;
    }
    return this._ackingLsn;
  }
  ackImmediate(lsn) {
    this._ackTimer.refresh();
    lsn = this.ack(lsn);
    if (this._tx.writableHighWaterMark > this._tx.writableLength) {
      return this._tx.write(updateStatusMessage(lsnInc(lsn)));
    }
  }
  end() {
    this.ackImmediate();
    this._tx.end();
    // return finishedp(this._rx);
  }
  _onData(msg) {
    if (msg.tag != 'CopyData') {
      return;
    }
    const replmsg = readReplicationMessage(msg.data);
    switch (replmsg.tag) {
      case 'XLogData':
        if (!this.push(replmsg)) {
          this._rx.pause();
        }
        return;
      case 'PrimaryKeepaliveMessage': {
        if (replmsg.shouldReply) {
          this.ackImmediate();
        }
      }
    }
  }
  _read() {
    this._rx.resume();
  }
}

function readReplicationMessage(buf) {
  const bufr = new BufferReader(buf);
  switch (bufr.read_char()) {
    case 'w': return {
      tag: 'XLogData',
      lsn: lsnBuf2hex(bufr.read_u64be()),
      endLsn: lsnBuf2hex(bufr.read_u64be()),
      time: bufr.read_u64be(),
      data: bufr.read_to_end(),
    };
    case 'k': return {
      tag: 'PrimaryKeepaliveMessage',
      endLsn: lsnBuf2hex(bufr.read_u64be()),
      time: bufr.read_u64be(),
      shouldReply: bufr.read_u8(),
    };
    default: throw new Error('Unknown replication message');
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
    const tag = bufr.read_u8();
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
      relationid: bufr.read_i32be(),
      schema: bufr.read_cstr(),
      name: bufr.read_cstr(),
      replicaIdentity: bufr.read_char(),
      attrs: Array(bufr.read_i16be()).fill(null).map(_ => ({
        flags: bufr.read_u8(),
        name: bufr.read_cstr(),
        typeid: bufr.read_i32be(),
        typemod: bufr.read_i32be(),
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
      typeid: bufr.read_i32be(),
      schema: bufr.read_cstr(),
      name: bufr.read_cstr(),
    };
  }
  _handleOrigin(_, bufr) {
    return {
      tag: 'origin',
      lsn: lsnBuf2hex(bufr.read_u64be()),
      origin: bufr.read_cstr(),
    };
  }
  _handleBegin({ lsn, endLsn, time }, bufr) {
    return {
      tag: 'begin',
      lsn,
      endLsn,
      time,
      finalLsn: lsnBuf2hex(bufr.read_u64be()),
      commitTime: bufr.read_u64be(),
      xid: bufr.read_i32be(),
    };
  }
  _handleCommit({ lsn, endLsn, time }, bufr) {
    return {
      tag: 'commit',
      lsn,
      endLsn,
      time,
      flags: bufr.read_u8(),
      commitLsn: lsnBuf2hex(bufr.read_u64be()),
      _endLsn: lsnBuf2hex(bufr.read_u64be()),
      commitTime: bufr.read_u64be(),
    };
  }
  _handleInsert({ lsn, endLsn, time }, bufr) {
    const relid = bufr.read_i32be();
    const _kind = bufr.read_u8();
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
    const relid = bufr.read_i32be();
    const attrs = this._attrs[relid];
    let kind = bufr.read_u8();
    let before = null;
    if (kind == 0x4b /*K*/ || kind == 0x4f /*O*/) {
      before = readTuple(bufr, attrs);
      kind = bufr.read_u8();
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
    const relid = bufr.read_i32be();
    const kind = bufr.read_u8();
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
    const nrels = bufr.read_i32be();
    const flags = bufr.read_u8();
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
        .map(_ => bufr.read_i32be())
        .map(relid => this._relations[relid])
      ),
    };
  }
}

function readTuple(bufr, attrs) {
  const n = bufr.read_i16be();
  const tuple = {}; // Object.create(null);
  for (let i = 0; i < n; i++) {
    const { name, decode } = attrs[i];
    switch (bufr.read_u8()) {
      case 0x74: // 't'
        tuple[name] = decode(bufr.read_countedstr());
        break;
      case 0x6e: // 'n'
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
  const lsn_buf = Buffer.from(a.padStart(8, '0') + b.padStart(8, '0'), 'hex');
  const msg = Buffer.allocUnsafe(1 + 8 + 8 + 8 + 8 + 1);
  msg.fill(0);
  msg[0] = 'r'.charCodeAt();
  lsn_buf.copy(msg, 1);
  lsn_buf.copy(msg, 1 + 8);
  lsn_buf.copy(msg, 1 + 8 + 8);
  return msg;
}

// use zero-padded lsn format because it is comparable
// by string comparer and is still valid pg_lsn representation
function lsnBuf2hex(buf) {
  return (
    buf.slice(0, 4).toString('hex').toUpperCase() +
    '/' + buf.slice(4).toString('hex').toUpperCase()
  );
}

function lsnInc(lsn) {
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
