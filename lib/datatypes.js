define({
  name: 'bool',
  id: 16,
  arrayid: 1000,
  decodeBin: buf => buf[0] == 1,
  decodeText: str => str == 't',
});

define({
  name: 'bytea',
  id: 17,
  arrayid: 1001,
  decodeBin: buf => buf,
  decodeText: str => Buffer.from(str.slice(2 /* skip \x */), 'hex'),
});

define({
  name: 'int8',
  id: 20,
  arrayid: 1016,
  decodeBin: buf => BigInt.asIntN(64, '0x' + buf.toString('hex')),
  decodeText: str => BigInt(str),
});

define({
  name: 'int2',
  id: 21,
  arrayid: 1005,
  decodeBin: buf => buf.readInt16BE(),
  decodeText: Number,
});

define({
  name: 'int4',
  id: 23,
  arrayid: 1007,
  decodeBin: buf => buf.readInt32BE(),
  decodeText: Number,
});

define({
  name: 'text',
  id: 25,
  arrayid: 1009,
  decodeBin: buf => buf.toString(),
  decodeText: str => str,
});

define({
  name: 'json',
  id: 114,
  arrayid: 199,
  decodeBin: JSON.parse,
  decodeText: JSON.parse,
});

define({
  name: 'jsonb',
  id: 3802,
  arrayid: 3807,
  decodeBin: buf => JSON.parse(buf.slice(1/* skip version byte */)),
  decodeText: JSON.parse,
});

exports.noop = {
  decodeBin: buf => buf,
  decodeText: str => str,
};


function define({ name, id, arrayid, decodeBin, decodeText }) {
  exports[name] = exports[id] = { id, decodeBin, decodeText };
  //exports[name + '[]'] = exports[arrayid] = array({ id: arrayid, decodeBin, decodeText });
}
