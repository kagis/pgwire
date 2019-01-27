const { Transform } = require('stream');
const BufferReader = require('./bufreader.js');

class BackendDecoder extends Transform {
  constructor() {
    super({
      readableObjectMode: true,
      readableHighWaterMark: 1000,
    });
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

      this.push(readMessage(
        String.fromCharCode(buf[0]),
        buf.slice(1 + 4, 1 + msg_len),
      ));

      buf = buf.slice(1 + msg_len);
    }
  }
}

function readMessage(ident, payload_buf) {
  const payload = new BufferReader(payload_buf);
  switch (ident) {
    case 'd': return {
      tag: 'CopyData',
      data: payload_buf,
    };
    case 'D': {
      const nfields = payload.read_i16be();
      const data = new Array(nfields);
      for (let i = 0; i < nfields; i++) {
        const len = payload.read_i32be();
        data[i] = len < 0 ? null : payload.read_exact(len);
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
        default: throw new Error('Unknown auth message');
      }
    case 'S': return {
      tag: 'ParameterStatus',
      parameter: payload.read_cstr(),
      value: payload.read_cstr(),
    };
    case 'K': return {
      tag: 'BackendKeyData',
      pid: payload.read_i32be(),
      secret: payload.read_i32be(),
    };
    case 'Z': return {
      tag: 'ReadyForQuery',
      transactionStatus: payload.read_u8(),
    };
    case 'H': return { tag: 'CopyOutResponse', ...readCopyResp(payload) };
    case 'G': return { tag: 'CopyInResponse', ...readCopyResp(payload) };
    case 'W': return { tag: 'CopyBothResponse', ...readCopyResp(payload) };
    case 'c': return { tag: 'CopyDone' };
    case 'T': return {
      tag: 'RowDescription',
      fields: Array(payload.read_i16be()).fill(0).map(_ => ({
        name: payload.read_cstr(),
        tableid: payload.read_i32be(),
        column: payload.read_i16be(),
        typeid: payload.read_i32be(),
        typelen: payload.read_i16be(),
        typemod: payload.read_i32be(),
        binary: payload.read_i16be(),
      })),
    };
    case 't': return {
      tag: 'ParameterDescription',
      typeids: (
        Array(payload.read_i16be())
        .fill(0)
        .map(_ => payload.read_i32be())
      ),
    };
    case 'C': return { tag: 'CommandComplete', command: payload.read_cstr() };
    case '1': return { tag: 'ParseComplete' };
    case '2': return { tag: 'BindComplete' };
    case '3': return { tag: 'CloseComplete' };
    case 's': return { tag: 'PortalSuspended' };
    case 'I': return { tag: 'EmptyQueryResponse' };
    case 'N': return { tag: 'NoticeResponse', ...readErrorOrNotice(payload) };
    case 'E': return { tag: 'ErrorResponse', ...readErrorOrNotice(payload) };
    case 'n': return { tag: 'NoData' };
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

function readErrorOrNotice(payload) {
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
    position: fields['P'] && Number(fields['P']),
    internalPosition: fields['p'] && Number(fields['p']),
    internalQuery: fields['q'],
    where: fields['W'],
    file: fields['F'],
    line: fields['L'] && Number(fields['L']),
    routine: fields['R'],
    schema: fields['s'],
    table: fields['t'],
    column: fields['c'],
    datatype: fields['d'],
    constraint: fields['n'],
  };
}

function readCopyResp(bufr) {
  return {
    binary: bufr.read_u8(),
    binaryPerAttr: (
      Array(bufr.read_i16be())
      .fill(0)
      .map(_ => bufr.read_i16be())
    ),
  };
}

module.exports = {
  BackendDecoder
};
