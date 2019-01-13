FROM postgres:11-alpine
RUN echo 'ALTER SYSTEM SET wal_level = logical;' >> /docker-entrypoint-initdb.d/0-config.sql \
 && echo "ALTER SYSTEM SET wal_sender_timeout = '10s';" >> /docker-entrypoint-initdb.d/0-config.sql \
 && echo 'pg_ctl restart -w -D "$PGDATA"' > /docker-entrypoint-initdb.d/1-restart.sh

