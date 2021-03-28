const { Transform } = require('stream');
const BufferReader = require('./bufreader.js');

module.exports =
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
      const msglen = buf.readUInt32BE(1);
      if (buf.length < 1 + msglen) {
        this._unconsumed = buf;
        return done();
      }

      this.push(readMessage(
        String.fromCharCode(buf[0]),
        buf.slice(1 + 4, 1 + msglen),
      ));

      buf = buf.slice(1 + msglen);
    }
  }
};

function readMessage(ident, buf) {
  const bufr = new BufferReader(buf);
  switch (ident) {
    case 'd': return {
      tag: 'CopyData',
      data: buf,
    };
    case 'D': return {
      tag: 'DataRow',
      data: readDataRow(bufr),
    };
    case 'R':
      switch (bufr.readInt32BE()) {
        case 0: return {
          tag: 'AuthenticationOk',
        };
        case 2: return {
          tag: 'AuthenticationKerberosV5',
        };
        case 3: return {
          tag: 'AuthenticationCleartextPassword',
        };
        case 5: return {
          tag: 'AuthenticationMD5Password',
          salt: bufr.readExact(4),
        };
        case 6: return {
          tag: 'AuthenticationSCMCredential',
        };
        case 7: return {
          tag: 'AuthenticationGSS',
        };
        case 8: return {
          tag: 'AuthenticationGSSContinue',
          data: bufr.readToEnd(),
        };
        case 9: return {
          tag: 'AuthenticationSSPI',
        };
        case 10: return {
          tag: 'AuthenticationSASL',
          mechanism: bufr.readCStr(),
        };
        case 11: return {
          tag: 'AuthenticationSASLContinue',
          data: bufr.readToEnd(),
        };
        case 12: return {
          tag: 'AuthenticationSASLFinal',
          data: bufr.readToEnd(),
        };
        default: throw Error('Unknown auth message');
      }
    case 'S': return {
      tag: 'ParameterStatus',
      parameter: bufr.readCStr(),
      value: bufr.readCStr(),
    };
    case 'K': return {
      tag: 'BackendKeyData',
      pid: bufr.readInt32BE(),
      secretkey: bufr.readInt32BE(),
    };
    case 'Z': return {
      tag: 'ReadyForQuery',
      transactionStatus: bufr.readUInt8(),
    };
    case 'H': return {
      tag: 'CopyOutResponse',
      ...readCopyResp(bufr),
    };
    case 'G': return {
      tag: 'CopyInResponse',
      ...readCopyResp(bufr),
    };
    case 'W': return {
      tag: 'CopyBothResponse',
      ...readCopyResp(bufr),
    };
    case 'c': return {
      tag: 'CopyDone',
    };
    case 'T': return {
      tag: 'RowDescription',
      fields: Array(bufr.readInt16BE()).fill(0).map(_ => ({
        name: bufr.readCStr(),
        tableid: bufr.readInt32BE(),
        column: bufr.readInt16BE(),
        typeid: bufr.readInt32BE(),
        typelen: bufr.readInt16BE(),
        typemod: bufr.readInt32BE(),
        binary: bufr.readInt16BE(),
      })),
    };
    case 't': return {
      tag: 'ParameterDescription',
      typeids: (
        Array(bufr.readInt16BE())
        .fill(0)
        .map(_ => bufr.readInt32BE())
      ),
    };
    case 'C': return {
      tag: 'CommandComplete',
      command: bufr.readCStr(),
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
    case 'N': return {
      tag: 'NoticeResponse',
      ...readErrorOrNotice(bufr),
    };
    case 'E': return {
      tag: 'ErrorResponse',
      ...readErrorOrNotice(bufr),
    };
    case 'n': return {
      tag: 'NoData',
    };
    case 'A': return {
      tag: 'NotificationResponse',
      pid: bufr.readInt32BE(),
      channel: bufr.readCStr(),
      payload: bufr.readCStr(),
    };
    default: return {
      tag: ident,
      body: buf,
    };
  }
}

function readDataRow(bufr) {
  const nfields = bufr.readInt16BE();
  const data = Array(nfields);
  for (let i = 0; i < nfields; i++) {
    const len = bufr.readInt32BE();
    data[i] = len < 0 ? null : bufr.readExact(len);
  }
  return data;
}

function readErrorOrNotice(bufr) {
  const fields = {};
  let fieldCode;
  while ((fieldCode = bufr.readUInt8()) > 0) {
    fields[String.fromCharCode(fieldCode)] = bufr.readCStr();
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
    binary: bufr.readUInt8(),
    binaryPerAttr: (
      Array(bufr.readInt16BE())
      .fill(0)
      .map(_ => bufr.readInt16BE())
    ),
  };
}
