#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: deploy/favn/runner-image-contract.sh IMAGE EXPECTED_RUNNER_RELEASE_ID" >&2
  exit 64
fi

image=$1
expected_release_id=$2

inspect() {
  docker image inspect --format "$1" "$image"
}

[[ $(inspect '{{.Os}}/{{.Architecture}}') == linux/amd64 ]]
[[ $(inspect '{{.Config.User}}') == 10001:10001 ]]
[[ $(inspect '{{.Config.WorkingDir}}') == /opt/favn ]]
[[ $(inspect '{{ index .Config.Labels "org.opencontainers.image.version" }}') == "$expected_release_id" ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.runner-release-id" }}') == "$expected_release_id" ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.elixir-version" }}') == 1.20.4 ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.otp-version" }}') == 29.0.6 ]]
duckdb_version=$(inspect '{{ index .Config.Labels "io.favn.duckdb-version" }}')
[[ -n $duckdb_version ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.target" }}') == linux/amd64 ]]
[[ $(inspect '{{json .Config.Entrypoint}}') == '["/opt/favn/bin/favn_runner"]' ]]
[[ $(inspect '{{json .Config.Healthcheck.Test}}') == '["CMD","/opt/favn/bin/favn_runner","rpc","case FavnRunner.readiness() do :ok -> :ok; other -> raise inspect(other) end"]' ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^FAVN_RUNNER_RELEASE_ID=') == "FAVN_RUNNER_RELEASE_ID=$expected_release_id" ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^DUCKDB_ADBC_DRIVER=') == "DUCKDB_ADBC_DRIVER=/opt/duckdb/$duckdb_version/libduckdb.so" ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LANG=') == LANG=C.UTF-8 ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LC_ALL=') == LC_ALL=C.UTF-8 ]]

image_environment=$(inspect '{{range .Config.Env}}{{println .}}{{end}}')
! grep -Ei '^[A-Z0-9_]*(TOKEN|PASSWORD|COOKIE|SECRET|DATABASE_URL|PIN_KEY|STORAGE_KEY|SAS)=' <<< "$image_environment" | grep -q .

image_history=$(docker image history --no-trunc --format '{{.CreatedBy}}' "$image")
! grep -Ei '(TOKEN|PASSWORD|COOKIE|SECRET_KEY_BASE|DATABASE_URL|PIN_KEY|STORAGE_KEY|SAS)=' <<< "$image_history" | grep -q .

contract=$(cat <<SH
set -eu
# Minimum Debian security fixes from the 14 September 2026 runtime snapshot.
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' libc6)" ge 2.41-12+deb13u4
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' libc-bin)" ge 2.41-12+deb13u4
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' perl-base)" ge 5.40.1-6+deb13u1
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' gzip)" ge 1.13-1+deb13u1
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' libpcre2-8-0)" ge 10.46-1~deb13u2
dpkg --compare-versions "\$(dpkg-query -W -f='\${Version}' libsqlite3-0)" ge 3.46.1-7+deb13u2
test "\$(id -u)" = 10001
test "\$(id -g)" = 10001
test -x /opt/favn/bin/favn_runner
test ! -e /opt/favn/releases/COOKIE
! find /opt/favn -user 10001 | grep -q .
! find /opt/favn -group 10001 | grep -q .
! find /opt/duckdb -user 10001 | grep -q .
! find /opt/duckdb -group 10001 | grep -q .
test -r /opt/duckdb/$duckdb_version/libduckdb.so
test -r /var/lib/favn/.duckdb/extensions/v$duckdb_version/linux_amd64/ducklake.duckdb_extension
test -r /var/lib/favn/.duckdb/extensions/v$duckdb_version/linux_amd64/postgres_scanner.duckdb_extension
test -r /var/lib/favn/.duckdb/extensions/v$duckdb_version/linux_amd64/json.duckdb_extension
test ! -w /opt/favn
test ! -w /opt/duckdb/$duckdb_version/libduckdb.so
test ! -w /var/lib/favn/.duckdb/extensions/v$duckdb_version/linux_amd64
! find /opt/favn /opt/duckdb /var/lib/favn/.duckdb -xdev \( -type f -o -type d \) -perm /0022 | grep -q .
! find /opt/favn -type f \( -name COOKIE -o -name .erlang.cookie \) | grep -q .
! find / -xdev -type f -perm /6000 -print 2>/dev/null | grep -q .
SH
)

docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --entrypoint /bin/sh \
  "$image" \
  -c "$contract"

# Prove the supported SQL path works without a writable image or network access.
# INSTALL must resolve the preinstalled extension instead of downloading it.
docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --env "EXPECTED_DUCKDB_VERSION=$duckdb_version" \
  --env FAVN_RUNNER_NODE_HOST_ALIAS=runner \
  --env FAVN_DISTRIBUTION_COOKIE=favn-runner-contract-7A9c2D4e6F8h0J1k \
  "$image" \
  eval '"1.20.4" = System.version(); ~c"17.0.6" = :erlang.system_info(:version); alias Favn.Connection.Resolved; alias Favn.SQL.Adapter.DuckDB.ADBC; resolved = %Resolved{name: :warehouse, adapter: ADBC, module: __MODULE__, config: %{open: [database: ":memory:"]}}; {:ok, conn} = ADBC.connect(resolved, []); {:ok, version} = ADBC.query(conn, "SELECT version() AS version", []); true = version.rows == [%{"version" => "v" <> System.fetch_env!("EXPECTED_DUCKDB_VERSION")}]; for extension <- ["ducklake", "postgres_scanner", "json"], do: ({:ok, _} = ADBC.execute(conn, "INSTALL #{extension}", [])); for extension <- ["ducklake", "postgres", "json"], do: ({:ok, _} = ADBC.execute(conn, "LOAD #{extension}", [])); {:ok, result} = ADBC.query(conn, "SELECT json_valid(?) AS valid", params: ["{}"]); true = result.rows == [%{"valid" => true}]; ADBC.disconnect(conn, []); IO.puts("duckdb-adbc-ok")'
