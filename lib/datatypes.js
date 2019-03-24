module.exports = {
  datatypesByName: Object.create(null),
  datatypesById: Object.create(null),
};

define({
  name: 'bool',
  id: 16,
  arrayid: 1000,
  decodeBin: buf => buf[0] == 1,
  decodeText: str => str == 't',
  encode: bool => bool ? 't' : 'f',
});

define({
  name: 'bytea',
  id: 17,
  arrayid: 1001,
  decodeBin: buf => buf,
  decodeText: str => Buffer.from(str.slice(2 /* skip \x */), 'hex'),
  encode: buf => Buffer.isBuffer(buf) ? buf : Buffer.from(buf),
  encodeBin: true,
});

define({
  name: 'int8',
  id: 20,
  arrayid: 1016,
  decodeBin: buf => BigInt.asIntN(64, '0x' + buf.toString('hex')),
  decodeText: str => BigInt(str),
  encode: String,
});

define({
  name: 'int2',
  id: 21,
  arrayid: 1005,
  decodeBin: buf => buf.readInt16BE(),
  decodeText: Number,
  encode: String,
});

define({
  name: 'int4',
  id: 23,
  arrayid: 1007,
  decodeBin: buf => buf.readInt32BE(),
  decodeText: Number,
  encode: String,
});

define({
  name: 'float4',
  id: 700,
  arrayid: 1021,
  decodeBin: buf => buf.readFloatBE(),
  decodeText: Number,
  encode: String,
});

define({
  name: 'float8',
  id: 701,
  arrayid: 1022,
  decodeBin: buf => buf.readDoubleBE(),
  decodeText: Number,
  encode: String,
});

define({
  name: 'text',
  id: 25,
  arrayid: 1009,
  decodeBin: String,
  decodeText: String,
  encode: String,
});

define({
  name: 'json',
  id: 114,
  arrayid: 199,
  decodeBin: JSON.parse,
  decodeText: JSON.parse,
  encode: JSON.stringify,
});

define({
  name: 'jsonb',
  id: 3802,
  arrayid: 3807,
  decodeBin: buf => JSON.parse(buf.slice(1/* skip version byte */)),
  decodeText: JSON.parse,
  encode: JSON.stringify,
});

define({
  name: 'pg_lsn',
  id: 3220,
  arrayid: 3221,
  decodeBin: buf => (buf.hexSlice(0, 4) + '/' + buf.hexSlice(4)).toUpperCase(),
  decodeText: str => str.split('/').map(it => it.padStart(8, '0')).join('/'),
  encode: String,
});

function define({ name, id, arrayid, decodeText, decodeBin, encode, encodeBin }) {
  module.exports.datatypesByName[name] =
  module.exports.datatypesById[id] = { id, decodeText, decodeBin, encode };
  module.exports.datatypesByName[name + '[]'] =
  module.exports.datatypesById[arrayid] = {
    id: arrayid,
    decodeBin: buf => decodeBinArray(buf, decodeBin),
    decodeText: str => decodeTextArray(str, decodeText),
    encode: encodeBin
      ? arr => encodeBinArray(arr, encode, id)
      : arr => encodeTextArray(arr, encode),
  };
}

function decodeTextArray(inp, decodeElem) {
  inp = inp.replace(/^\[.+=/, ''); // skip dimensions
  const jsonArray = inp.replace(/{|}|,|"(?:[^"\\]|\\.)*"|[^,}]+/gy, token => (
    token == '{' ? '[' :
    token == '}' ? ']' :
    token == 'NULL' ? 'null' :
    token == ',' || token[0] == '"' ? token :
    JSON.stringify(token)
  ));
  return JSON.parse(jsonArray,
    (_, elem) => typeof elem == 'string' ? decodeElem(elem) : elem
  );
}

function decodeBinArray(buf, decodeElem) {
  const ndim = buf.readInt32BE();
  let cardinality = 0;
  for (let di = ndim - 1; di >= 0; di--) {
    cardinality += buf.readInt32BE(12 + di * 8);
  }
  let result = Array(cardinality);
  for (let pos = 12 + ndim * 8, i = 0; pos < buf.length; i++) {
    const len = buf.readInt32BE(pos);
    pos += 4;
    if (len < 0) {
      result[i] = null;
    } else {
      result[i] = decodeElem(buf.slice(pos, pos += len));
    }
  }
  for (let di = ndim - 1; di > 0; di--) {
    const dimlen = buf.readInt32BE(12 + di * 8);
    const reshaped = Array(result.length / dimlen);
    for (let i = 0; i < reshaped.length; i++) {
      reshaped[i] = result.slice(i * dimlen, (i + 1) * dimlen);
    }
    result = reshaped;
  }
  return result;
}

// FIXME: one dimension only
function encodeTextArray(arr, encodeElem) {
  return JSON.stringify(arr, function (_, elem) {
    return this == arr && elem != null ? encodeElem(elem) : elem;
  }).replace(/^\[(.*)]$/, '{$1}');
}

// FIXME: one dimension only
function encodeBinArray(array, encodeElem, elemTypeid) {
  const ndim = 1;
  const encodedArray = Array(array.length);
  let size = 4 + 4 + 4 + ndim * (4 + 4) + array.length * 4;
  let hasNull = 0;
  for (let i = 0; i < array.length; i++) {
    if (array[i] == null) {
      hasNull = 1;
    } else {
      const elBuf = encodeElem(array[i]);
      size += elBuf.length;
      encodedArray[i] = elBuf;
    }
  }
  const result = Buffer.allocUnsafe(size);
  let pos = 0;
  pos = result.writeInt32BE(1, pos);
  pos = result.writeInt32BE(hasNull, pos);
  pos = result.writeInt32BE(elemTypeid, pos);
  pos = result.writeInt32BE(array.length, pos);
  const lb = 1;
  pos = result.writeInt32BE(lb, pos);
  for (const elBuf of encodedArray) {
    if (elBuf) {
      pos = result.writeInt32BE(elBuf.length, pos);
      pos += elBuf.copy(result, pos);
    } else {
      pos += result.writeInt32BE(-1, pos);
    }
  }
  return result;
}
