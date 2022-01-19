# pgoutput messages


```js
for await (const chunk of replstream.pgoutputDecode()) {

  // (string) Lsn of last received message.
  // Use it for replstream.ack() to confirm receipt of whole chunk.
  chunk.lastLsn;
  // (bigint) Time of last received message. Microseconds since unix epoch.
  chunk.lastTime;

  for (const pgomsg of chunk.messages) {
    // (string | null) Log Serial Number of message.
    // Use it for replstream.ack() to confirm receipt of message.
    // TODO describe nullability.
    pgomsg.lsn;
    // (bigint) The server's system clock at the time of transmission,
    // as microseconds since Unix epoch.
    pgomsg.time;

    switch (pgomsg.tag) {

      // Transaction start boundary.
      case 'begin':
        // (string) Equals to `.commitLsn` of following `commit` message
        // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275
        pgomsg.commitLsn;
        // (bigint) Microseconds since unix epoch.
        pgomsg.commitTime;
        // (number) Transaction id.
        pgomsg.xid;

      case 'origin':
        // (string)
        pgomsg.originName;
        // (string)
        pgomsg.originLsn;

      // Emitted for user defined types which are used
      // in following `relation` message
      case 'type':
        // (string)
        pgomsg.typeOid;
        // (string)
        pgomsg.typeSchema;
        // (string)
        pgomsg.typeName;

      // Relation (table) description.
      // Emmited once per relation before
      // `insert`/`update`/`delete`/`truncate` messages
      // which references this relation.
      case 'relation':
        // (number) pg_class reference.
        pgomsg.relationOid;
        // (string) Relation (table) schema name
        pgomsg.schema;
        // (string) Relation (table) name
        pgomsg.name:
        // ('default' | 'nothing'| 'full' | 'index')
        // https://www.postgresql.org/docs/14/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY
        pgomsg.replicaIdentity;
        // (string[]) Array of key attribute names (attr.flags & 0b1)
        pgomsg.keyNames;
        // (object[]) Relation attributes (table columns) descriptions
        for (const attr of pgomsg.attrs) {
          // (number) 0b1 if attribute is part of replica identity
          attr.flags;
          // (string)
          attr.name;
          // (number)
          attr.typeOid;
          // (number)
          attr.typeMod;
          // (string | null)
          attr.typeName;
          // (string | null)
          attr.typeSchema;
        }

      case 'insert':
        // (object) Associated relation.
        pgomsg.relation;
        // (object) Key attributes values.
        pgomsg.key;
        // (object) Inserted row values.
        pgomsg.after;

      case 'update':
        // (object) Associated relation.
        pgomsg.relation;
        // (object) Key attributes values.
        pgomsg.key;
        // (object | null) If pgomsg.relation.replicaIdentity == 'full'
        // then gets row values before update, otherwise gets null
        pgomsg.before;
        // (object) Row values after update.
        // If pgomsg.relation.replicaIdentity != 'full'
        // then unchanged TOASTed values will be undefined.
        // https://www.postgresql.org/docs/14/storage-toast.html
        pgomsg.after;

      case 'delete':
        // (object) Associated relation.
        pgomsg.relation;
        // (object) Key attributes values.
        pgomsg.key;
        // (object | null) If pgomsg.relation.replicaIdentity == 'full'
        // then gets deleted row values, otherwise gets null
        pgomsg.before;

      case 'truncate':
        // (boolean)
        pgomsg.cascade;
        // (boolean)
        pgomsg.restartIdentity;
        // (object[]) Truncated relations descriptions.
        pgomsg.relations;

      // pg_logical_emit_message
      // https://www.postgresql.org/docs/14/functions-admin.html#id-1.5.8.33.8.5.2.2.22.1.1.1
      case 'message':
        // (string)
        pgomsg.prefix;
        // (Uint8Array)
        pgomsg.content;
        // (string) Equals to lsn which `pg_logical_emit_message` returns.
        pgomsg.messageLsn;
        // (boolean)
        pgomsg.transactional;

      // Transaction end boundary.
      case 'commit':
        // (string) Equals to `.commitLsn` of preceding `begin` message.
        pgomsg.commitLsn;
        // (bigint) Microseconds since unix epoch.
        pgomsg.commitTime;
    }
  }
}
```
