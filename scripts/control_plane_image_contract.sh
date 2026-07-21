#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: scripts/control_plane_image_contract.sh IMAGE CONTROL_PLANE_BUILD_ID" >&2
  exit 64
fi

image=$1
build_id=$2

if [[ ! $build_id =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid control-plane build id" >&2
  exit 64
fi

inspect() {
  docker image inspect --format "$1" "$image"
}

[[ $(inspect '{{.Os}}/{{.Architecture}}') == linux/amd64 ]]
[[ $(inspect '{{.Config.User}}') == 10001:10001 ]]
[[ $(inspect '{{.Config.WorkingDir}}') == /app ]]
[[ $(inspect '{{ index .Config.Labels "org.opencontainers.image.source" }}') == https://github.com/eirhop/favn ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.control-plane.build-id" }}') == "$build_id" ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.elixir-version" }}') == 1.20.2 ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.otp-version" }}') == 28.3.3 ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.target" }}') == linux/amd64 ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LANG=') == LANG=C.UTF-8 ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LC_ALL=') == LC_ALL=C.UTF-8 ]]
[[ $(inspect '{{json .Config.Entrypoint}}') == '["/app/bin/favn_control_plane"]' ]]
[[ $(inspect '{{json .Config.Healthcheck.Test}}') == '["CMD","/app/bin/favn_control_plane_health"]' ]]

contract=$(cat <<'SH'
set -eu
test "$(id -u)" = 10001
test "$(id -g)" = 10001
test -x /app/bin/favn_control_plane
test -x /app/bin/favn_control_plane_health
test -x /app/bin/favn_control_plane_ops
test -f /app/control-plane-build.json
test "$(cat /app/runtime-versions/ELIXIR_VERSION)" = 1.20.2
test "$(cat /app/runtime-versions/OTP_VERSION)" = 28.3.3
test ! -e /app/releases/COOKIE
! find /app -type f \( -name COOKIE -o -name .erlang.cookie \) | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_core-*' | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_view-*' | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_orchestrator-*' | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_storage_postgres-*' | grep -q .
! find /app/lib -maxdepth 1 -type d -name 'favn_runner-*' | grep -q .
! find /app/lib -maxdepth 1 -type d -name 'favn_local-*' | grep -q .
! find /app/lib -maxdepth 1 -type d -name 'favn_authoring-*' | grep -q .
! find /app/lib -maxdepth 1 -type d -name 'favn_test_support-*' | grep -q .
! find /app/lib -maxdepth 1 -type d -name 'mix-*' | grep -q .
! find /app -type f -name '*.ex' | grep -q .
! find /app -type f -name '*.exs' ! -path '/app/releases/*/runtime.exs' | grep -q .
! find /app -type f \( -name '*.eex' -o -name '*.heex' \) | grep -q .
! find /app -type f -name '*.map' | grep -q .
! grep -R -l '"sourcesContent"' /app | grep -q .
! find /app/lib -path '*/phoenix-*/priv/templates' -type d | grep -q .
! find /app -type f -name 'mix.exs' | grep -q .
! find /app -type d \( -name deps -o -name _build -o -name .git \) | grep -q .
! grep -F '/build/' /app/releases/*/sys.config | grep -q .
! grep -E '\{(esbuild|tailwind),' /app/releases/*/sys.config | grep -q .
SH
)

docker run --rm \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --entrypoint /bin/sh \
  "$image" \
  -c "$contract"

docker run --rm --entrypoint /bin/sh "$image" -c \
  "grep -F '\"control_plane_build_id\":\"$build_id\"' /app/control-plane-build.json >/dev/null"

control_plane_version=$(inspect '{{ index .Config.Labels "org.opencontainers.image.version" }}')
manifest_schema=$(inspect '{{ index .Config.Labels "io.favn.manifest-schema-version" }}')
runner_contract=$(inspect '{{ index .Config.Labels "io.favn.runner-contract-version" }}')

docker run --rm --env FAVN_EXPECTED_VERSION="$control_plane_version" --entrypoint /bin/sh "$image" -c \
  'grep -F "\"control_plane_version\":\"$FAVN_EXPECTED_VERSION\"" /app/control-plane-build.json >/dev/null'
docker run --rm --env FAVN_EXPECTED_SCHEMA="$manifest_schema" --entrypoint /bin/sh "$image" -c \
  'grep -F "\"manifest_schema_version\":$FAVN_EXPECTED_SCHEMA" /app/control-plane-build.json >/dev/null'
docker run --rm --env FAVN_EXPECTED_RUNNER_CONTRACT="$runner_contract" --entrypoint /bin/sh "$image" -c \
  'grep -F "\"runner_contract_version\":$FAVN_EXPECTED_RUNNER_CONTRACT" /app/control-plane-build.json >/dev/null'

docker run --rm --entrypoint /bin/sh "$image" -c \
  "file=\$(find /app/lib -path '*/favn_view-*/priv/static/cache_manifest.json' -type f -print -quit); test -n \"\$file\"; sha256sum \"\$file\" | cut -d ' ' -f 1"

assert_launcher_rejects() {
  local expected=$1 node=$2 cookie=$3 output status

  set +e
  output=$(docker run --rm \
    --env "FAVN_CONTROL_PLANE_NODE=$node" \
    --env "FAVN_DISTRIBUTION_COOKIE=$cookie" \
    --env FAVN_BEAM_DISTRIBUTION_PORT=9101 \
    "$image" eval ':ok' 2>&1)
  status=$?
  set -e

  [[ $status -eq 1 ]]
  [[ $output == *"$expected"* ]]
}

valid_cookie=favn-control-cookie-7A9c2D4e6F8h0J1k
assert_launcher_rejects "invalid FAVN_CONTROL_PLANE_NODE" "control@localhost" "$valid_cookie"
assert_launcher_rejects "invalid FAVN_CONTROL_PLANE_NODE" "control@@internal" "$valid_cookie"
assert_launcher_rejects "invalid FAVN_DISTRIBUTION_COOKIE" "control@control.internal" "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

utf8_output=$(docker run --rm \
  --env FAVN_CONTROL_PLANE_NODE=control@control.internal \
  --env "FAVN_DISTRIBUTION_COOKIE=$valid_cookie" \
  --env FAVN_BEAM_DISTRIBUTION_PORT=9101 \
  "$image" eval 'IO.puts("utf8-ok")' 2>&1)
[[ $utf8_output == *"utf8-ok"* ]]
[[ $utf8_output != *"native name encoding of latin1"* ]]
