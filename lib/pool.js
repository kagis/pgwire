const Connection = require('./connection.js');

module.exports =
class Pool {
  constructor({
    poolMaxConnections,
    poolResetQuery,
    ...options
  }) {
    this._options = options;
    this._connections = [];
    this._poolResetQuery = poolResetQuery;
    this._poolMaxConnections = Math.max(poolMaxConnections, 0);
  }

  query(...args) {
    const conn = this._getConnection();
    try {
      return conn.query(...args).on('readyForQuery', onReadyForQuery);
    } finally {
      this._releaseConnection(conn);
    }

    function onReadyForQuery({ transactionStatus }) {
      if (transactionStatus != 0x49/*I*/) {
        conn.destroy(new Error('Leave in transaction'));
      }
    }
  }

  logicalReplication(...args) {
    const conn = new Connection({
      ...this._options,
      replication: 'database',
    });
    return conn.logicalReplication(...args).finally(_ => {
      conn.end();
    });
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
    if (!this._poolMaxConnections) {
      return new Connection(this._options);
    }
    const leastBusyConn = this._connections.reduce(
      (acc, it) => acc && acc.queueSize < it.queueSize ? acc : it,
      null,
    );
    const hasSpaceForNewConnection = (
      this._connections.length < this._poolMaxConnections
    );
    if (
      leastBusyConn && leastBusyConn.queueSize == 0 ||
      !hasSpaceForNewConnection
    ) {
      return leastBusyConn;
    }

    const newConn = new Connection(this._options);
    this._connections.push(newConn);
    newConn.on('close', this._onConnectionClose.bind(this, newConn));
    return newConn;
  }

  _releaseConnection(conn) {
    if (!this._poolMaxConnections) {
      return conn.end();
    }
    if (this._poolResetQuery) {
      conn.query(this._poolResetQuery).catch(err => {
        conn.destroy(err);
      });
    }
  }

  _onConnectionClose(conn, _err) {
    this._connections = this._connections.filter(it => it != conn);
  }
};
