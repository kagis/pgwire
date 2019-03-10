FROM postgres:11-alpine
RUN echo 'ALTER SYSTEM SET wal_level = logical;' >> /docker-entrypoint-initdb.d/config.sql \
 && echo "ALTER SYSTEM SET wal_sender_timeout = '10s';" >> /docker-entrypoint-initdb.d/config.sql \
 && echo "CREATE ROLE u_clear LOGIN PASSWORD 'qwerty';" >> /docker-entrypoint-initdb.d/config.sql \
 && echo "CREATE ROLE u_md5 LOGIN PASSWORD 'qwerty';" >> /docker-entrypoint-initdb.d/config.sql \
 && echo "CREATE ROLE u_sha256 LOGIN PASSWORD 'qwerty';" >> /docker-entrypoint-initdb.d/config.sql \
 && echo "echo 'host all postgres all trust' > /var/lib/postgresql/data/pg_hba.conf" >> /docker-entrypoint-initdb.d/init-pg_hba.sh \
 && echo "echo 'host all u_clear all password' >> /var/lib/postgresql/data/pg_hba.conf" >> /docker-entrypoint-initdb.d/init-pg_hba.sh \
 && echo "echo 'host all u_md5 all md5' >> /var/lib/postgresql/data/pg_hba.conf" >> /docker-entrypoint-initdb.d/init-pg_hba.sh \
 && echo "echo 'host all u_sha256 all scram-sha-256' >> /var/lib/postgresql/data/pg_hba.conf" >> /docker-entrypoint-initdb.d/init-pg_hba.sh \
