const { pgconnect } = require('./connect.js');
const { pgliteral, pgident, pgarray } = require('./escape.js');

Object.assign(exports, {
  pgconnect,
  pgliteral,
  pgident,
  pgarray,
});
