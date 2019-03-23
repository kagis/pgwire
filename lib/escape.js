module.exports = {
  pgliteral,
  pgident,
};

function pgliteral(s) {
  if (s == null) {
    return 'NULL';
  }
  return (
    String(s)
    .replace(/'/g, '\'\'')
    .replace(/.*/, '\'$&\'')
  );
}

function pgident(...segments) {
  return (
    segments
    .map(it => (
      it.replace(/"/g, '""')
      .replace(/.*/, '"$&"')
    ))
    .join('.')
  );
}
