#!/bin/sh
set -eu

install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/certs
install -o postgres -g postgres -m 0600 \
  /certificates/postgres.key \
  /var/lib/postgresql/certs/server.key
install -o postgres -g postgres -m 0644 \
  /certificates/postgres.crt \
  /var/lib/postgresql/certs/server.crt
install -o postgres -g postgres -m 0644 \
  /certificates/ca.crt \
  /var/lib/postgresql/certs/ca.crt

exec docker-entrypoint.sh "$@"
