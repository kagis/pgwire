const { Readable, Transform, PassThrough, pipeline, finished } = require('stream');
const BufferReader = require('./bufreader.js');
const { pgliteral, pgident } = require('./escape.js');

Object.assign(exports, {
  logicalReplication,
});

async function logicalReplication(conn, {
  slot,
  startLsn, // todo validate lsn format
  ackIntervalMs,
  options = {},
}) {
  if (ackIntervalMs == null) {
    const { scalar } = await conn.query(
      `SELECT setting::int ` +
      `FROM pg_catalog.pg_settings ` +
      `WHERE name ='wal_sender_timeout'`
    );
    ackIntervalMs = scalar / 2;
  }
  const optionsSql = (
    Object.entries(options)
    .map(([k, v]) => k + ' ' + pgliteral(v))
    .join(',')
    .replace(/.+/, '($&)')
  );
  const tx = new PassThrough();
  const rx = conn.queryStream({
    sql: `START_REPLICATION SLOT ${pgident(slot)} LOGICAL ${startLsn} ${optionsSql}`,
    stdin: tx,
  });
  // wait for CopyBothResponse
  await new Promise((resolve, reject) => {
    rx.on('error', reject);
    rx.on('data', function ondata({ tag }) {
      if (tag == 'CopyBothResponse') {
        rx.off('data', ondata);
        rx.pause();
        return resolve();
      }
    })
  });
  return new ReplicationStream({ rx, tx, startLsn, ackIntervalMs });
}

class ReplicationStream extends Readable {
  constructor({ rx, tx }) {
    super({ objectMode: true });
    this._ackingLsn = '0/0';
    this._ackTimer = setTimeout(this.ackImmediate.bind(this), 2e3);
    this._tx = tx;
    finished(this._tx, _ => clearTimeout(this._ackTimer));
    this._rx = rx;
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
      return this._tx.write(updateStatusMessage(lsnHex2buf(lsn)));
    }
  }
  end() {
    return this._tx.end();
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
  }
  _transform({ lsn, endLsn, time, data }, _enc, done) {
    const bufr = new BufferReader(data);
    const tag = bufr.read_char();
    switch (tag) {
      case 'R': {
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
        return done(null, {
          tag: 'relation',
          lsn,
          endLsn,
          time,
          ...rel,
        });
      };
      case 'Y': return done(null, {
        tag: 'type',
        typeid: bufr.read_i32be(),
        schema: bufr.read_cstr(),
        name: bufr.read_cstr(),
      });
      case 'O': return done(null, {
        type: 'origin',
        lsn: lsnBuf2hex(bufr.read_u64be()),
        origin: bufr.read_cstr(),
      });
      case 'B': return done(null, {
        tag: 'begin',
        lsn,
        endLsn,
        time,
        finalLsn: lsnBuf2hex(bufr.read_u64be()),
        commitTime: bufr.read_u64be(),
        xid: bufr.read_i32be(),
      });
      case 'I': return done(null, {
        tag: 'insert',
        lsn,
        endLsn,
        time,
        relation: this._relations[bufr.read_i32be()],
        after: (bufr.read_u8(), readTuple(bufr)),
      });
      case 'D': return done(null, {
        tag: 'delete',
        lsn,
        endLsn,
        time,
        relation: this._relations[bufr.read_i32be()],
        keyOnly: bufr.read_char() == 'K',
        before: readTuple(bufr),
      });
      case 'U': return done(null, {
        tag: 'update',
        lsn,
        endLsn,
        time,
        relation: this._relations[bufr.read_i32be()],
        before: (
          'KO'.includes(bufr.peek_char()) &&
          bufr.read_char() &&
          readTuple(bufr) ||
          null
        ),
        after: bufr.read_char() && readTuple(bufr),
      });
      case 'T': {
        const nrels = bufr.read_i32be();
        const flags = bufr.read_u8();
        return done(null, {
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
        });
      }
      case 'C': return done(null, {
        tag: 'commit',
        lsn,
        endLsn,
        time,
        flags: bufr.read_u8(),
        commitLsn: lsnBuf2hex(bufr.read_u64be()),
        _endLsn: lsnBuf2hex(bufr.read_u64be()),
        commitTime: bufr.read_u64be(),
      });
      default: return done(null, { tag, lsn, endLsn, time, data });
    }
  }
}

function readTuple(bufr) {
  const n = bufr.read_i16be();
  const tuple = Array(n);
  for (let i = 0; i < n; i++) {
    switch (bufr.read_u8()) {
      case 0x74: // 't'
        tuple[i] = bufr.read_countedstr();
        break;
      case 0x6e: // 'n'
        tuple[i] = null;
        break;
      default: // 'u'
        tuple[i] = undefined;
    }
  }
  return tuple;
}

function updateStatusMessage(lsn) {
  const msg = Buffer.allocUnsafe(1 + 8 + 8 + 8 + 8 + 1);
  msg.fill(0);
  msg[0] = 'r'.charCodeAt();
  lsn.copy(msg, 1);
  lsn.copy(msg, 1 + 8);
  lsn.copy(msg, 1 + 8 + 8);
  return msg;
};

// use zero-padded lsn format because it is comparable
// by string comparer and is still valid pg_lsn representation
function lsnBuf2hex(buf) {
  return (
    buf.slice(0, 4).toString('hex').toUpperCase() +
    '/' + buf.slice(4).toString('hex').toUpperCase()
  );
}

function lsnHex2buf(hex) {
  const [a, b] = hex.split('/');
  return Buffer.from(a.padStart(8, '0') + b.padStart(8, '0'), 'hex');
}

// function lsn_buf2u64(buf) {
//   return BigInt('0x' + buf.toString('hex'));
// }

// function lsn_u64_2buf(u64) {
//   return Buffer.from(u64.toString(16).padStart(16, '0'), 'hex');
// }
