#!/usr/bin/env bash
# Local development PostgreSQL; owns only this repository's native cluster.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
data_dir="$root/.state/postgres-native"
socket_dir="$root/.state/postgres-socket"
log_file="$root/.state/postgres-native.log"
port=${PGPORT:-54329}
case "$port" in ''|*[!0-9]*) echo 'PGPORT must be numeric.' >&2; exit 2 ;; esac
action=${1:-start}
if [[ $# -gt 1 || ! "$action" =~ ^(start|stop|status)$ ]]; then
  echo 'Usage: bash scripts/postgres-runtime.sh [start|stop|status]' >&2; exit 2
fi
if command -v pg_ctl >/dev/null 2>&1; then
  pg_bin=$(dirname "$(command -v pg_ctl)")
elif [[ -x /opt/homebrew/opt/postgresql@17/bin/pg_ctl ]]; then
  pg_bin=/opt/homebrew/opt/postgresql@17/bin
else
  echo 'PostgreSQL 17 is required. Install postgresql@17 or use make postgres-docker.' >&2; exit 1
fi
if [[ "$action" == status ]]; then
  if [[ ! -f "$data_dir/PG_VERSION" ]]; then echo 'Prototype PostgreSQL cluster is not initialized.'; exit 3; fi
  exec "$pg_bin/pg_ctl" -D "$data_dir" status
fi
if [[ "$action" == stop ]]; then
  if [[ ! -f "$data_dir/PG_VERSION" ]] || ! "$pg_bin/pg_ctl" -D "$data_dir" status >/dev/null 2>&1; then
    echo 'Prototype PostgreSQL cluster is already stopped.'; exit 0
  fi
  exec "$pg_bin/pg_ctl" -D "$data_dir" -m fast -w -t 20 stop
fi
mkdir -p "$data_dir" "$socket_dir"
chmod 700 "$data_dir" "$socket_dir"
if [[ ! -f "$data_dir/PG_VERSION" ]]; then
  password_file=$(mktemp "$root/.state/pg-init-password.XXXXXX")
  trap 'rm -f "$password_file"' EXIT
  chmod 600 "$password_file"
  printf '%s\n' nexus_batch > "$password_file"
  "$pg_bin/initdb" -D "$data_dir" --username=nexus_batch --auth-local=trust --auth-host=scram-sha-256 --pwfile="$password_file" --encoding=UTF8 --locale=C
  rm -f "$password_file"
  trap - EXIT
fi
if ! "$pg_bin/pg_ctl" -D "$data_dir" status >/dev/null 2>&1; then
  "$pg_bin/pg_ctl" -D "$data_dir" -l "$log_file" -o "-h 127.0.0.1 -p $port -k \"$socket_dir\"" -w -t 20 start
fi
exists=$(PGPASSWORD=nexus_batch "$pg_bin/psql" -X -h 127.0.0.1 -p "$port" -U nexus_batch -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='nexus_batch'")
if [[ "$exists" != 1 ]]; then
  PGPASSWORD=nexus_batch "$pg_bin/createdb" -h 127.0.0.1 -p "$port" -U nexus_batch nexus_batch
fi
echo "Prototype PostgreSQL is ready on 127.0.0.1:$port (database nexus_batch)."
