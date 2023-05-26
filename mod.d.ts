export function pgconnect(...optionsChain: PgConnectOptions[]): Promise<PgConnection>;
export function pgconnection(...optionsChain: PgConnectOptions[]): PgConnection;
export function pgpool(...optionsChain: PgConnectOptions[]): PgClient;

export type PgConnectOptions = string | URL | PgConnectKnownOptions & Record<string, string | Uint8Array>;

export interface PgConnectKnownOptions {
  readonly hostname?: string;
  readonly port?: number;
  readonly sslmode?: 'require' | 'prefer' | 'allow' | 'disable' | null;
  // readonly sslrootcert?: string;
  readonly password?: string | Uint8Array;

  // these parameters are included in StatupMessage,
  // any runtime config parameters are allowed
  // https://www.postgresql.org/docs/14/runtime-config-client.html
  readonly user?: string | Uint8Array;
  readonly database?: string | Uint8Array;
  readonly replication?: string | Uint8Array;
  readonly application_name?: string | Uint8Array;

  // underscore parameters are pgwire specific parameters

  /** Connection attempts duration. If 0 (default) then only one connection attempt will be made. */
  readonly _connectRetry?: number | string;
  readonly _wakeInterval?: number | string;
  readonly _poolIdleTimeout?: number | string;
  readonly _poolSize?: number;
  readonly _debug?: boolean;
}

export interface PgClient {
  /** Simple query protocol. */
  query(script: string, options?: PgSimpleQueryOptions): Promise<PgResult>;
  /** Simple query protocol. */
  stream(script: string, options?: PgSimpleQueryOptions): AsyncIterableIterator<PgChunk>;

  /** Extended query protocol. */
  query(...args: Statement[] | [...Statement[], PgExtendedQueryOptions]): Promise<PgResult>;
  /** Extended query protocol. */
  stream(...args: Statement[] | [...Statement[], PgExtendedQueryOptions]): AsyncIterableIterator<PgChunk>;

  /** Terminates client gracefully if possible and waits until pending queries complete.
   * New queries will be rejected. Has no effect if client already ended or destroyed. */
  end(): Promise<void>;
  /** Terminates client abruptly. Pending queries will be rejected.
   * Has no effect when called on destroyed connection.
   * @param reason Pending queries will be rejected with provided reason.
   * New queries will also be rejected with provided reason unless `.end` was called
   * before `.destroy`.
   * @returns reason back so you can destroy and throw in one line. */
  destroy(reason?: any): Error;
}

export interface PgConnection extends PgClient {
  logicalReplication(options: LogicalReplicationOptions): ReplicationStream;
  /** Number of pending queries. */
  readonly pending: number;
  /** ID of postgres backend process. */
  readonly pid: number | null;
  readonly inTransaction: number | null;
  readonly ssl: boolean | null;
  /** Notification handler */
  onnotification: (n: PgNotification) => void;
  parameters: Record<string,string>;
}

export interface PgSimpleQueryOptions {
  readonly stdin?: AsyncIterable<Uint8Array>;
  readonly stdins?: Iterable<AsyncIterable<Uint8Array>>;
  readonly signal?: AbortSignal | null;
}

export interface PgExtendedQueryOptions {
  readonly signal?: AbortSignal | null;
}

export interface PgResult extends Iterable<any> {
  /**
   * @deprecated Use iterator instead.
   *
   * First row first column value. `undefined` if no rows returned. */
  readonly scalar: any;
  readonly rows: any[][];
  readonly columns: ColumnDescription[];
  /** - Command tag (`'SELECT ...'`, `'UPDATE ...'`) if CommandComplete received.
   * - `'PortalSuspended'` if {@link Statement.limit} has been reached.
   * - `'EmptyQueryResponse'` if statement contains whitespaces or comments only.
   * - `null` if there were more than one statement. */
  readonly status: string | 'PortalSuspended' | 'EmptyQueryResponse' | null;
  readonly results: PgSubResult[];
  readonly notices: PgNotice[];
}

export interface PgSubResult {
  /**
   * @deprecated
   *
   * First row first column value. `undefined` if no rows returned. */
  readonly scalar: any;
  readonly rows: any[][];
  readonly columns: ColumnDescription[];
  /** - Command tag (`'SELECT ...'`, `'UPDATE ...'`) if CommandComplete received.
   * - `'PortalSuspended'` if {@link Statement.limit} has been reached.
   * - `'EmptyQueryResponse'` if statement contains whitespaces or comments only. */
  readonly status: string | 'PortalSuspended' | 'EmptyQueryResponse';
}

export type PgChunk = (
  | PgChunkDataRow
  | PgChunkCopyData
  | PgChunkCommandComplete
  | PgChunkRowDescription
);

export interface PgChunkDataRow extends Uint8Array {
  readonly tag: 'DataRow';
  readonly rows: any[][];
  readonly copies: [];
  readonly payload: null;
}

export interface PgChunkCopyData extends Uint8Array  {
  readonly tag: 'CopyData';
  readonly rows: [];
  readonly copies: Uint8Array[];
  readonly payload: null;
}

export interface PgChunkCommandComplete extends Uint8Array  {
  readonly tag: 'CommandComplete';
  readonly rows: [];
  readonly copies: [];
  /** Command, SELECT N, UPDATE 0 N, ... */
  readonly payload: string;
}

export interface PgChunkRowDescription extends Uint8Array  {
  readonly tag: 'RowDescription';
  readonly rows: [];
  readonly copies: [];
  readonly payload: ColumnDescription[];
}

export interface ColumnDescription {
  readonly name: string;
  readonly typeOid: number;
  readonly typeMod: number;
}

export interface Statement {
  readonly statement: string;
  // readonly statementName?: string;
  readonly params?: StatementParam[];
  /** Max number of rows to fetch.
   * {@link StatementResult.suspended} will be `true` if limit has been reached. */
  readonly limit?: number;
  readonly stdin?: AsyncIterable<Uint8Array>;
}

export interface StatementParam {
  /** Valid type oid or builtin type name. */
  readonly type?: number | 'uuid' | 'varchar' | 'bool' | 'bytea' | 'int2' | 'int4' | 'float4' | 'float8' | 'int8' | 'json' | 'jsonb' | 'pg_lsn';
  readonly value: any;
}

export interface LogicalReplicationOptions {
  readonly slot: string;
  readonly startLsn?: string;
  /** Decoder options */
  readonly options?: Record<string, string>;
  readonly ackIntervalMillis?: number;
}

export interface ReplicationStream extends AsyncIterable<ReplicationChunk<ReplicationMessage>> {
  /** Confirms receipt of replication message by lsn.
   * Use {@link ReplicationMessage.lsn} to get lsn. */
  ack(lsn: string): undefined;
  /**
   * Decodes {@link ReplicationMessage.data} and yields upgraded pgoutput messages.
   * Use this method if replication is started with pgoutput slot. */
  pgoutputDecode(): AsyncIterable<ReplicationChunk<PgoutputMessage>>;
}

export interface ReplicationChunk<M> {
  readonly messages: M[];
  readonly lastLsn: string;
  readonly lastTime: bigint;
}

export type ReplicationMessage = (
  | ReplicationXLogData
  | ReplicationPrimaryKeepaliveMessage
);

export interface ReplicationMessageBase {
  /** Log Serial Number of message.
   * Use it for {@link ReplicationStream.ack} to confirm receipt of message. */
   readonly lsn: string | null;
   readonly endLsn: string | null;
   /** microseconds since unix epoch */
   readonly time: bigint;
}

export interface ReplicationXLogData extends ReplicationMessageBase {
  readonly tag: 'XLogData';
  /** binary payload */
  readonly data: Uint8Array;
}

export interface ReplicationPrimaryKeepaliveMessage extends ReplicationMessageBase  {
  readonly tag: 'PrimaryKeepaliveMessage';
  readonly shouldReply: number;
}

/** https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html */
export type PgoutputMessage = (
  | PgoutputBegin
  | PgoutputCommit
  | PgoutputRelation
  | PgoutputInsert
  | PgoutputUpdate
  | PgoutputDelete
  | PgoutputTruncate
  | PgoutputCustomMessage
  | ReplicationPrimaryKeepaliveMessage
);

export interface PgoutputBegin extends ReplicationMessageBase {
  readonly tag: 'begin';
  /** https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275 */
  readonly commitLsn: string;
  readonly commitTime: bigint;
  readonly xid: number;
}

export interface PgoutputCommit extends ReplicationMessageBase {
  readonly tag: 'commit';
  readonly commitLsn: string;
  readonly commitTime: bigint;
}

export interface PgoutputRelation extends ReplicationMessageBase {
  readonly tag: 'relation';
  readonly relationid: number;
  readonly schema: string;
  readonly name: string;
  /** https://www.postgresql.org/docs/14/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY */
  readonly replicaIdentity: 'default' | 'nothing'| 'full' | 'index';
  readonly columns: Array<{
    /** `0b1` if attribute is part of replica identity */
    readonly flags: number;
    readonly name: string;
    readonly typeOid: number;
    readonly typeMod: number;
    readonly typeSchema: string | null;
    readonly typeName: string | null;
  }>;
}

export interface PgoutputInsert extends ReplicationMessageBase {
  readonly tag: 'insert';
  readonly relation: PgoutputRelation;
  readonly before: null;
  readonly after: Record<string, any>;
}

export interface PgoutputUpdate extends ReplicationMessageBase {
  readonly tag: 'update';
  readonly relation: PgoutputRelation;
  /**
   * If {@link PgoutputRelation.replicaIdentity} == 'full'
   * then gets row values before update. */
  readonly before: Record<string, any> | null;
  /**
   * Gets row values after update.
   * If {@link PgoutputRelation.replicaIdentity} != 'full'
   * then unchanged TOASTed values will be `undefined`.
   * See https://www.postgresql.org/docs/14/storage-toast.html for TOASTing */
  readonly after: Record<string, any>;
}

export interface PgoutputDelete extends ReplicationMessageBase {
  readonly tag: 'delete';
  readonly relation: PgoutputRelation;
  /**
   * If {@link PgoutputRelation.replicaIdentity} == 'full'
   * then gets values of deleted row, otherwise gets `null`. */
  readonly before: Record<string, any> | null;
  readonly after: null;
}

export interface PgoutputTruncate extends ReplicationMessageBase {
  readonly tag: 'truncate';
  readonly cascade: boolean;
  readonly restartIdentity: boolean;
  /** Truncated relations. */
  readonly relations: PgoutputRelation[];
}

export interface PgoutputCustomMessage extends ReplicationMessageBase {
  readonly tag: 'message';
  readonly transactional: boolean;
  readonly messageLsn: string;
  readonly prefix: string;
  readonly content: Uint8Array;
}

/** https://www.postgresql.org/docs/14/protocol-error-fields.html */
export interface PgNotice {
  /** WARNING, NOTICE, DEBUG, INFO, or LOG, or a localized translation of one of these. */
  severity: string;
  /** The SQLSTATE code for the error. Not localizable.
   * https://www.postgresql.org/docs/14/errcodes-appendix.html */
  code: string;
  /** The primary human-readable error message. This should be accurate but terse (typically one line). */
  message: string;
  /** Optional secondary error message carrying more detail about the problem. Might run to multiple lines. */
  detail: string | undefined;
  /** An optional suggestion what to do about the problem.
   * This is intended to differ from Detail in that it offers advice
   * (potentially inappropriate) rather than hard facts.
   * Might run to multiple lines. */
  hint: string | undefined;
  /** Error cursor position as an index into the original query string.
   * The first character has index 1, and positions are measured in characters not bytes. */
  position: number | undefined;
  internalPosition: number | undefined;
  internalQuery: string | undefined;
  where: string | undefined;
  file: string | undefined;
  line: string | undefined;
  routine: string | undefined;
  schema: string | undefined;
  table: string | undefined;
  column: string | undefined;
  datatype: string | undefined;
  constraint: string | undefined;
}

export interface PgNotification {
  readonly pid: number;
  readonly channel: string;
  readonly payload: string;
}
