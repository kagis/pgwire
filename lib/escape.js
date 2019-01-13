Object.assign(exports, {
  pgliteral,
  pgident,
  pgarray,
});

function pgliteral(s) {
  if (s == null) {
    return 'NULL'
  }
  return '\'' + s.toString().replace(/'/g, '\'\'') + '\'';
}

function pgident(...segments) {
  // безусловно оборачиваем строку в кавычки потому
  // что иначе нужно проверять на ключевые слова
  // типа user которые нельзя использовать в качестве имен
  return segments.map(it => '"' + it.replace(/"/g, '""') + '"').join('.');
}

function pgarray(arr) {
  return '{' + JSON.stringify(arr).slice(1, -1) + '}';
}
