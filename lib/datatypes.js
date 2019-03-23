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

function define({ name, id, arrayid, decodeText, decodeBin, encode }) {
  module.exports.datatypesByName[name] =
  module.exports.datatypesById[id] = { id, decodeText, decodeBin, encode };
  module.exports.datatypesByName[name + '[]'] =
  module.exports.datatypesById[arrayid] = {
    id: arrayid,
    decodeBin: noop => noop,
    decodeText: noop => noop,
    encode: arr => JSON.stringify(arr.map(encode)).replace(/^\[(.*)]$/, '{$1}'),
  };
}

