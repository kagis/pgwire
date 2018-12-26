#!/bin/sh
psql <<- SQL
ALTER SYSTEM SET wal_level = logical;
SQL
pg_ctl restart -w -D "$PGDATA"
