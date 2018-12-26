exports.pg_literal = function pg_literal(s) {
  return '\'' + s.replace(/'/g, '\'\'') + '\'';
}

exports.pg_ident = function pg_ident(...segments) {
  // безусловно оборачиваем строку в кавычки потому
  // что иначе нужно проверять на ключевые слова
  // типа user которые нельзя использовать в качестве имен
  return segments.map(it => '"' + it.replace(/"/g, '""') + '"').join('.');
}

exports.pg_array = function pg_array(arr) {
  return '{' + JSON.stringify(arr).slice(1, -1) + '}';
}
