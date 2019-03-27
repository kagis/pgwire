const Connection = require('./connection.js');

module.exports =
class Pool {
  constructor({ poolMaxConnections, ...options }) {
    this._options = options;
    this._connections = [];
    this._poolMaxConnections = Number(poolMaxConnections) || 0;
  }

  query(options) {
    const conn = this._getConnection();
    try {
      return conn.query(options);
    } finally {
      this._releaseConnection(conn);
    }
  }

  async logicalReplication(options) {
    const conn = new Connection({
      ...this._options,
      replication: 'database',
    });
    try {
      return await conn.logicalReplication(options);
    } finally {
      conn.end();
    }
  }

  async session(fn) {
    const conn = new Connection(this._options);
    try {
      return await fn(conn);
    } finally {
      conn.end();
    }
  }

  clear() {
    for (const conn of this._connections) {
      conn.end();
    }
  }

  _getConnection() {
    if (this._poolMaxConnections == 0) {
      return new Connection(this._options);
    }
    let conn = this._connections.reduce(
      (acc, it) => acc && acc.queueSize > it.queueSize ? acc : it,
      null,
    );
    if (
      !(conn && conn.queueSize == 0 /* is reusable */) &&
      this._connections.length < this._poolMaxConnections
    ) {
      conn = new Connection(this._options);
      this._connections.push(conn);
      conn.on('error', _err => null);
      conn.on('close', this._onConnectionClose.bind(this, conn));
    }
    return conn;
  }

  _releaseConnection(conn) {
    if (this._poolMaxConnections == 0) {
      return conn.end();
    }
    Promise.all([
      // do one query per statement for case of replication=database
      // which not support multi-statement query
      conn.query(/*sql*/ `ROLLBACK`),
      conn.query(/*sql*/ `DISCARD ALL`),
    ])
    .catch(_err => {
      conn.end();
    });
  }

  _onConnectionClose(conn, _err) {
    this._connections = this._connections.filter(it => it != conn);
  }
};
