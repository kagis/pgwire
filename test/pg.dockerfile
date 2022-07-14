FROM alpine:3.15
ENV PGDATA=/var/lib/postgresql/data
RUN set -x \
 && apk add --no-cache postgresql14 openssl \
 && install -o postgres -g postgres -m 700 -d /run/postgresql
USER postgres
WORKDIR $PGDATA
RUN initdb
RUN set -x \

 && printf %s\\n \
  "authorityKeyIdentifier=keyid,issuer" \
  "basicConstraints=CA:FALSE" \
  "keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment" \
  "subjectAltName = DNS:pgwssl" \
  > domains.txt \

 # I understand almost nothing about openssl,
 # got this from https://deno.land/x/postgres@v0.15.0/docker/generate_tls_keys.sh
 && openssl req -x509 -nodes -new -sha256 -newkey rsa:2048 -keyout ca.key -out ca.pem -subj "/CN=Example-Root-CA" \
 && openssl x509 -outform pem -in ca.pem -out ca.crt \
 && openssl req -new -nodes -newkey rsa:2048 -keyout server.key -out server.csr -subj "/CN=Example" \
 && openssl x509 -req -sha256 -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -extfile domains.txt -out server.crt \
 && chmod og-rwx server.key \

 && printf %s\\n \
  "listen_addresses = '0.0.0.0'" \
  "wal_level = logical" \
  "log_timezone = UTC" \
  "timezone = UTC" \
  "ssl = on" \
  > postgresql.conf \

 && printf %s\\n \
  #           db  user              addr method
  "host       all postgres          all  trust" \
  "host       all pgwire            all  trust" \
  "host       all pgwire_pwd        all  password" \
  "host       all pgwire_md5        all  md5" \
  "host       all pgwire_sha256     all  scram-sha-256" \
  "hostssl    all pgwire_sslonly    all  trust" \
  "hostnossl  all pgwire_nossl      all  trust" \
  "local      all all                    trust" \
  > pg_hba.conf \

 && pg_ctl --wait start \
 &&  printf %s\\n \
  " create role pgwire login superuser; " \
  " create role pgwire_pwd login password 'secret'; " \
  " set password_encryption = 'md5'; " \
  " create role pgwire_md5 login password 'secret'; " \
  " set password_encryption = 'scram-sha-256'; " \
  " create role pgwire_sha256 login password 'secret'; " \
  " create role pgwire_sslonly login; " \
  " create role pgwire_nossl login; " \
  | psql -v ON_ERROR_STOP=1 \
 && pg_ctl --wait stop

CMD ["postgres"]
