#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "usage: scripts/control_plane_image_contract.sh IMAGE [EXPECTED_VERSION] [EXPECTED_REVISION]" >&2
  exit 64
fi

image=$1
expected_version=${2:-}
expected_revision=${3:-}

inspect() {
  docker image inspect --format "$1" "$image"
}

[[ $(inspect '{{.Os}}/{{.Architecture}}') == linux/amd64 ]]
[[ $(inspect '{{.Config.User}}') == 10001:10001 ]]
[[ $(inspect '{{.Config.WorkingDir}}') == /app ]]
[[ $(inspect '{{ index .Config.Labels "org.opencontainers.image.source" }}') == https://github.com/eirhop/favn ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.elixir-version" }}') == 1.20.2 ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.otp-version" }}') == 29.0.4 ]]
[[ $(inspect '{{ index .Config.Labels "io.favn.target" }}') == linux/amd64 ]]
label_version=$(inspect '{{ index .Config.Labels "org.opencontainers.image.version" }}')
label_revision=$(inspect '{{ index .Config.Labels "org.opencontainers.image.revision" }}')
label_manifest_schema=$(inspect '{{ index .Config.Labels "io.favn.manifest-schema-version" }}')
label_runner_contract=$(inspect '{{ index .Config.Labels "io.favn.runner-contract-version" }}')
[[ -n $label_version ]]
[[ -n $label_revision ]]
[[ -n $label_manifest_schema ]]
[[ -n $label_runner_contract ]]
if [[ -n $expected_version ]]; then [[ $label_version == "$expected_version" ]]; fi
if [[ -n $expected_revision ]]; then [[ $label_revision == "$expected_revision" ]]; fi
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LANG=') == LANG=C.UTF-8 ]]
[[ $(inspect '{{range .Config.Env}}{{println .}}{{end}}' | grep '^LC_ALL=') == LC_ALL=C.UTF-8 ]]
[[ $(inspect '{{json .Config.Entrypoint}}') == '["/app/bin/favn_control_plane"]' ]]
[[ $(inspect '{{json .Config.Healthcheck.Test}}') == '["CMD","/app/bin/favn_control_plane_health"]' ]]

image_environment=$(inspect '{{range .Config.Env}}{{println .}}{{end}}')
! grep -Ei '^[A-Z0-9_]*(TOKEN|PASSWORD|COOKIE|SECRET|DATABASE_URL|PIN_KEY|STORAGE_KEY|SAS)=' <<< "$image_environment" | grep -q .

image_history=$(docker image history --no-trunc --format '{{.CreatedBy}}' "$image")
! grep -Ei '(TOKEN|PASSWORD|COOKIE|SECRET_KEY_BASE|DATABASE_URL|PIN_KEY|STORAGE_KEY|SAS)=' <<< "$image_history" | grep -q .

embedded_metadata=$(docker run --rm --entrypoint /bin/sh "$image" -c \
  'printf "%s|%s|%s" "$(cat /app/runtime-versions/FAVN_VERSION)" "$(cat /app/runtime-versions/MANIFEST_SCHEMA_VERSION)" "$(cat /app/runtime-versions/RUNNER_CONTRACT_VERSION)"')
[[ $embedded_metadata == "$label_version|$label_manifest_schema|$label_runner_contract" ]]

contract=$(cat <<'SH'
set -eu
test "$(id -u)" = 10001
test "$(id -g)" = 10001
test -d "$HOME"
test -x /app/bin/favn_control_plane
test -x /app/bin/favn_control_plane_health
test -x /app/bin/favn_control_plane_ops
test "$(cat /app/runtime-versions/ELIXIR_VERSION)" = 1.20.2
test "$(cat /app/runtime-versions/OTP_VERSION)" = 29.0.4
test ! -e /app/releases/COOKIE
! find /app -user 10001 | grep -q .
! find /app -group 10001 | grep -q .
! find /app -type f \( -name COOKIE -o -name .erlang.cookie \) | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_core-*' | grep -q .
find /app/lib -maxdepth 1 -type d -name 'favn_azure-*' | grep -q .
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
! find /app -xdev \( -type f -o -type d \) -perm /0022 | grep -q .
! grep -F '/build/' /app/releases/*/sys.config | grep -q .
! grep -E '\{(esbuild|tailwind),' /app/releases/*/sys.config | grep -q .
! find / -xdev -type f -perm /6000 -print 2>/dev/null | grep -q .
SH
)

docker run --rm \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --entrypoint /bin/sh \
  "$image" \
  -c "$contract"

docker run --rm --entrypoint /bin/sh "$image" -c \
  "file=\$(find /app/lib -path '*/favn_view-*/priv/static/cache_manifest.json' -type f -print -quit); test -n \"\$file\"; sha256sum \"\$file\" | cut -d ' ' -f 1"

set +e
status_stdout=$(docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --entrypoint /app/bin/favn_control_plane_ops \
  "$image" status)
status_exit=$?
set -e
if [[ $status_exit -ne 64 ]]; then
  echo "status contract returned exit $status_exit instead of 64" >&2
  exit 1
fi
status_nonempty_lines=$(printf '%s\n' "$status_stdout" | awk 'NF { count++ } END { print count + 0 }')
if [[ $status_nonempty_lines -ne 1 ]]; then
  echo "status contract stdout contained $status_nonempty_lines non-empty lines instead of one" >&2
  exit 1
fi
if ! grep -Eq '^\{.*\}$' <<< "$status_stdout"; then
  echo "status contract stdout was not one JSON object" >&2
  exit 1
fi
if ! grep -Fq '"operation":"status"' <<< "$status_stdout"; then
  echo "status contract stdout omitted the operation" >&2
  exit 1
fi
if ! grep -Fq '"state":"invalid_configuration"' <<< "$status_stdout"; then
  echo "status contract stdout omitted the invalid configuration state" >&2
  exit 1
fi

assert_launcher_rejects() {
  local expected=$1 node=$2 cookie=$3 output status

  set +e
  output=$(docker run --rm \
    --env "FAVN_CONTROL_PLANE_NODE=$node" \
    --env "FAVN_DISTRIBUTION_COOKIE=$cookie" \
    --env FAVN_BEAM_DISTRIBUTION_PORT=9101 \
    "$image" start 2>&1)
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
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  "$image" eval 'true = File.dir?(System.fetch_env!("HOME")); IO.puts("utf8-ok")' 2>&1)
[[ $utf8_output == *"utf8-ok"* ]]
[[ $utf8_output != *"native name encoding of latin1"* ]]
