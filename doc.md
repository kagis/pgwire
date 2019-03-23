# Extended query protocol

```js
.query({
  extended: true,
  script: [{
    sql: 'SELECT $1, $2',
    params: [{
      type: 'int4',
      value: 42,
    }, {
      type: 25,
      value: 'hello',
    }],
    limit: 10,
    stdin: fs.createFileStream(...)
  }, {
    // creates prepared statement
    op: 'parse',
    // sql statement body
    sql: 'SELECT $1, $2',
    // optional statement name
    // if not specified then unnamed statement will be created
    statementName: 'example-statement',
    // optional parameters types hints
    // if not specified then parameter types will be inferred
    paramTypes: [
      // can use builtin postgres type names
      'int4',
      // also can use any type oid directly
      25,
    ],
  }, {
    // creates portal (cursor) from prepared statement
    op: 'bind',
    // optional statement name
    // if not specified then last unnamed prepared statement
    // will be used
    statementName: 'example-statement',
    // optional portal name
    // if not specified then unnamed portal will be created
    portal: 'example-portal',
    // optional parameters list
    params: [{
      // optional builtin parameter type name
      // if not specified then value should be
      // string or Buffer representing raw text or binary
      // encoded value
      type: 'int4',
      // $1 parameter value
      value: 1,
    }, {
      // also can use builtin type oid
      type: 25,
      // $2 parameter value
      value: 'hello',
    }],
  }, {
    // executes portal
    op: 'execute',
    // optional portal name
    // if not specified then last unnamed portal will be executed
    portal: 'example-portal',
    // optional row limit value
    // if not specified then all rows will be returned
    // if limit is exceeded then result will have `suspeded: true` flag
    limit: 10,
    // optional stdin source stream for `COPY target FROM STDIN` queries
    stdin: fs.createFileStream(...),
  }],
});
```
