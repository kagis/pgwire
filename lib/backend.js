const { Transform } = require('stream');
const BufferReader = require('./bufreader.js');

class BackendDecoder extends Transform {
  constructor() {
    super({ readableObjectMode: true });
    this._unconsumed = Buffer.allocUnsafe(0);
  }
  _transform(buf, _encoding, done) {
    if (this._unconsumed.length) {
      buf = Buffer.concat([this._unconsumed, buf]);
    }

    for (;;) {
      if (buf.length < 1 + 4) {
        this._unconsumed = buf;
        return done();
      }
      const msg_len = buf.readUInt32BE(1);
      if (buf.length < 1 + msg_len) {
        this._unconsumed = buf;
        return done();
      }

      this.push(read_message(
        String.fromCharCode(buf[0]),
        buf.slice(1 + 4, 1 + msg_len),
      ));

      buf = buf.slice(1 + msg_len);
    }
  }
}

exports.BackendDecoder = BackendDecoder;

function read_message(ident, payload_buf) {
  const payload = new BufferReader(payload_buf);
  switch (ident) {
    case 'd': return {
      tag: 'CopyData',
      data: payload_buf,
    };
    case 'D': {
      const field_count = payload.read_i16be();
      const data = new Array(field_count);
      for (let i = 0; i < field_count; i++) {
        const val_len = payload.read_i32be();
        data[i] = val_len < 0 ? null : payload.read_exact(val_len).toString();
      }
      return {
        tag: 'DataRow',
        data,
      };
    }
    case 'R':
      switch (payload.read_i32be()) {
        case 0: return { tag: 'AuthenticationOk' };
        case 2: return { tag: 'AuthenticationKerberosV5' };
        case 3: return { tag: 'AuthenticationCleartextPassword' };
        case 5: return {
          tag: 'AuthenticationMD5Password',
          salt: payload.read_exact(4),
        };
        case 6: return { tag: 'AuthenticationSCMCredential' };
        case 7: return { tag: 'AuthenticationGSS' };
        case 9: return { tag: 'AuthenticationSSPI' };
      }
    case 'S': return {
      tag: 'ParameterStatus',
      parameter: payload.read_cstr(),
      value: payload.read_cstr(),
    };
    case 'K': return {
      tag: 'BackendKeyData',
      process_id: payload.read_i32be(),
      secret_key: payload.read_i32be(),
    };
    case 'Z': return {
      tag: 'ReadyForQuery',
      transaction_status: payload.read_u8(),
    };
    case 'H': return {
      tag: 'CopyOutResponse',
      binary: payload.read_u8(),
      att_binary: (
        Array(payload.read_i16be())
        .fill(0)
        .map(_ => payload.read_i16be())
      ),
    };
    case 'G': return {
      tag: 'CopyInResponse',
      binary: payload.read_u8(),
      att_binary: (
        Array(payload.read_i16be())
        .fill(0)
        .map(_ => payload.read_i16be())
      ),
    };
    case 'c': return {
      tag: 'CopyDone',
    };
    case 'T': return {
      tag: 'RowDescription',
      fields: Array(payload.read_i16be()).fill(0).map(_ => ({
        name: payload.read_cstr(),
        table_oid: payload.read_i32be(),
        table_col: payload.read_i16be(),
        type_oid: payload.read_i32be(),
        type_len: payload.read_i16be(),
        type_mod: payload.read_i32be(),
        format: payload.read_i16be(),
      })),
    };
    case 't': return {
      tag: 'ParameterDescription',
      typeoids: (
        Array(payload.read_i16be())
        .fill(0)
        .map(_ => payload.read_i32be())
      ),
    };
    case 'C': return {
      tag: 'CommandComplete',
      command_tag: payload.read_cstr(),
    };
    case '1': return {
      tag: 'ParseComplete',
    };
    case '2': return {
      tag: 'BindComplete',
    };
    case '3': return {
      tag: 'CloseComplete',
    };
    case 's': return {
      tag: 'PortalSuspended',
    };
    case 'I': return {
      tag: 'EmptyQueryResponse',
    };
    case 'W': return {
      tag: 'CopyBothResponse',
      overall_format: payload.read_u8(),
      columns_format: (
        Array(payload.read_i16be())
        .fill(0)
        .map(_ => payload.read_i16be())
      ),
    };
    case 'N': return {
      tag: 'NoticeResponse',
      ...read_error_or_notice(payload),
    };
    case 'E': return {
      tag: 'ErrorResponse',
      ...read_error_or_notice(payload),
    };
    case 'A': return {
      tag: 'NotificationResponse',
      pid: payload.read_i32be(),
      channel: payload.read_cstr(),
      payload: payload.read_cstr(),
    };
    default: return {
      tag: ident,
      body: payload_buf,
    };
  }
}

function read_error_or_notice(payload) {
  const fields = {};
  let field_code;
  while ((field_code = payload.read_u8()) > 0) {
    fields[String.fromCharCode(field_code)] = payload.read_cstr();
  }
  return {
    severity: fields['S'],
    code: fields['C'],
    message: fields['M'],
    detail: fields['D'],
    hint: fields['H'],
    position: fields['P'] && +fields['P'],
    internal_position: fields['p'] && Number(fields['p']),
    internal_query: fields['q'],
    where: fields['W'],
    file: fields['F'],
    line: fields['L'] && Number(fields['L']),
    routine: fields['R'],
    schema_name: fields['s'],
    table_name: fields['t'],
    column_name: fields['c'],
    datatype_name: fields['d'],
    constraint_name: fields['n'],
  };
}
