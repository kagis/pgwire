const { Transform } = require('stream');
const BufferReader = require('./bufreader.js');

function read_replication_message(buf) {
  const bufr = new BufferReader(buf);
  switch (bufr.read_char()) {
    case 'w': return {
      tag: 'XLogData',
      lsn: bufr.read_u64be(),
      end_lsn: bufr.read_u64be(),
      time: bufr.read_u64be(),
      data: bufr.read_to_end(),
    };
    case 'k': return {
      tag: 'PrimaryKeepaliveMessage',
      end_lsn: bufr.read_u64be(),
      time: bufr.read_u64be(),
      should_reply: bufr.read_u8(),
    };
    default: throw new Error('Unknown replication message');
  }
}

exports.read_pgoutput_message = function read_pgoutput_message(buf) {
  const bufr = new BufferReader(buf);
  switch (bufr.read_char()) {
    case 'I': return {
      tag: 'insert',
      table_oid: bufr.read_i32be(),
      newtuple: (bufr.read_u8(), read_tuple(bufr)),
    };
    case 'D': return {
      tag: 'delete',
      table_oid: bufr.read_i32be(),
      oldtuple_keyonly: bufr.read_char() == 'K',
      oldtuple: read_tuple(bufr),
    };
    case 'U': return {
      tag: 'update',
      table_oid: bufr.read_i32be(),
      oldtuple: (
        'KO'.includes(bufr.peek_char()) &&
        bufr.read_char() &&
        read_tuple(bufr)
      ),
      newtuple: bufr.read_char() && read_tuple(bufr),
    };
    case 'B': return {
      tag: 'begin',
      final_lsn: bufr.read_u64be(),
      commit_time: bufr.read_u64be(),
      xid: bufr.read_i32be(),
    };
    case 'C': return {
      tag: 'commit',
      flags: bufr.read_u8(),
      commit_lsn: bufr.read_u64be(),
      end_lsn: bufr.read_u64be(),
      commit_time: bufr.read_u64be(),
    };
    case 'R': return {
      tag: 'relation',
      table_oid: bufr.read_i32be(),
      schema: bufr.read_cstr(),
      name: bufr.read_cstr(),
      replica_identity: bufr.read_char(),
      attrs: Array(bufr.read_i16be()).fill(null).map(_ => ({
        flags: bufr.read_u8(),
        name: bufr.read_cstr(),
        type_oid: bufr.read_i32be(),
        type_mod: bufr.read_i32be(),
      })),
    };
    case 'Y': return {
      tag: 'type',
      type_oid: bufr.read_i32be(),
      schema: bufr.read_cstr(),
      name: bufr.read_cstr(),
    };
    case 'O': return {
      type: 'origin',
      lsn: bufr.read_u64be(),
      origin: bufr.read_cstr(),
    };
    case 'T': {
      const ntable_oids = bufr.read_i32be();
      const flags = bufr.read_u8();
      return {
        tag: 'truncate',
        cascade: flags & 0b1,
        restart_seqs: flags & 0b10,
        table_oids: (
          Array(ntable_oids)
          .fill(0)
          .map(_ => bufr.read_i32be())
        ),
      };
    }
  }
}

function read_tuple(bufr) {
  return Array(bufr.read_i16be()).fill(null).map(_ => {
    switch (bufr.read_char()) {
      case 'n': return null;
      case 'u': return undefined;
      case 't': return bufr.read_countedstr();
      default: throw new Error('Unknown repr');
    }
  });
}

exports.replication_update_status_msg = function replication_update_status_msg(lsn) {
  const msg = Buffer.allocUnsafe(1 + 8 + 8 + 8 + 8 + 1);
  msg.fill(0);
  msg[0] = 0x72; // r
  lsn.copy(msg, 1);
  lsn.copy(msg, 1 + 8);
  lsn.copy(msg, 1 + 8 + 8);
  return msg;
};

exports.ReplicationStream = class ReplicationStream extends Transform {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 1000,
    });
  }

  _transform(msg, _encoding, done) {
    if (msg.tag != 'CopyData') {
      return done();
    }
    const replication_msg = read_replication_message(msg.data);
    switch (replication_msg.tag) {
      case 'XLogData': return done(null, replication_msg);
      // case 'PrimaryKeepaliveMessage': {
      //   if (replication_msg.should_reply) {
      //     // this._send_feedback();
      //   }
      // }
    }
    // console.log(replication_msg);
    return done();
  }
}

exports.lsn_buf2hex = function lsn_buf2hex(buf) {
  return (
    buf.slice(0, 4).toString('hex').toUpperCase() +
    '/' + buf.slice(4).toString('hex').toUpperCase()
  );
}

exports.lsn_hex2buf = function lsn_hex2buf(hex) {
  const [a, b] = hex.split('/');
  return Buffer.from(a.padStart(8, '0') + b.padStart(8, '0'), 'hex');
}

exports.lsn_buf2u64 = function lsn_buf2u64(buf) {
  return BigInt('0x' + buf.toString('hex'));
}

exports.lsn_u64_2buf = function lsn_u64_2buf(u64) {
  return Buffer.from(u64.toString(16).padStart(16, '0'), 'hex');
}
