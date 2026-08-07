#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH='' cd -- "$script_dir/../.." && pwd)
compose_file="$script_dir/compose.yml"
security_compose_file="$script_dir/compose.security.yml"
env_file="$script_dir/.env.local"
results_dir="$script_dir/security-results"
secrets_dir="$script_dir/security/secrets"
admin_password_file="$secrets_dir/admin-password"
owns_generated_state=0

cleanup() {
  if [ "$owns_generated_state" != "1" ] ||
      [ "${FAVN_SECURITY_KEEP_STACK:-0}" = "1" ] ||
      [ ! -f "$env_file" ]; then
    return
  fi

  if command -v compose >/dev/null 2>&1; then
    compose --profile security --profile proxy-security down \
      --timeout "${FAVN_COMPOSE_STOP_TIMEOUT_SECONDS:-10}" \
      --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  fi
  sh "$script_dir/prune-qualification-images.sh" "$env_file" || true
  rm -f "$admin_password_file" "$env_file"
}

trap cleanup EXIT HUP INT TERM

if [ -f "$env_file" ]; then
  echo "$env_file already exists; inspect or clean the existing disposable project first" >&2
  exit 1
fi

source_revision=${FAVN_SOURCE_REVISION:-$(git -C "$repository_root" rev-parse HEAD)}
if ! printf '%s\n' "$source_revision" | grep -Eq '^[0-9a-f]{40}$'; then
  echo "FAVN_SOURCE_REVISION must be a full lowercase Git revision" >&2
  exit 1
fi
head_revision=$(git -C "$repository_root" rev-parse HEAD)
if [ "$source_revision" != "$head_revision" ]; then
  echo "FAVN_SOURCE_REVISION must identify the checked-out HEAD" >&2
  exit 1
fi

source_state=clean
if [ -n "$(git -C "$repository_root" status --porcelain --untracked-files=normal)" ]; then
  if [ "${FAVN_SECURITY_ALLOW_DIRTY:-0}" != "1" ]; then
    echo "security qualification requires a clean checkout" >&2
    exit 1
  fi
  source_state=dirty_diagnostic
fi

short_revision=$(printf '%s' "$source_revision" | cut -c 1-12)
run_suffix="$(date -u +%Y%m%d%H%M%S)-$$"
export FAVN_COMPOSE_PROJECT_NAME="favn-security-$short_revision-$run_suffix"
export FAVN_SOURCE_REVISION="$source_revision"
export FAVN_SECURITY_SOURCE_STATE="$source_state"
if [ "$source_state" = "dirty_diagnostic" ]; then
  export FAVN_QUALIFICATION_ALLOW_DIRTY=1
  export FAVN_IMAGE_TAG="diagnostic-$short_revision-$run_suffix"
fi

sh "$script_dir/prepare.sh"
owns_generated_state=1

project_name=$(
  sed -n 's/^FAVN_COMPOSE_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)
image_build_project_name=$(
  sed -n 's/^FAVN_IMAGE_BUILD_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)
image_builder_name=$(
  sed -n 's/^FAVN_IMAGE_BUILDER_NAME=//p' "$env_file" |
    head -n 1
)

for name in "$project_name" "$image_build_project_name" "$image_builder_name"; do
  case "$name" in
    ""|*[!a-z0-9_-]*)
      echo "refusing to use an invalid Compose project name" >&2
      exit 1
      ;;
  esac
done

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$project_name" \
    --file "$compose_file" \
    --file "$security_compose_file" \
    "$@"
}

build_compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$image_build_project_name" \
    --file "$compose_file" \
    --file "$security_compose_file" \
    --profile security \
    --profile proxy-security \
    "$@"
}

build_images() {
  if build_compose build --help 2>/dev/null | grep -q -- '--provenance'; then
    build_compose build --builder "$image_builder_name" --provenance=false "$@"
  else
    export BUILDX_NO_DEFAULT_ATTESTATIONS=1
    build_compose build --builder "$image_builder_name" "$@"
  fi
}

record() {
  assertion_id=$1
  outcome=$2
  details=$3
  printf '{"schema_version":1,"assertion_id":"%s","suite":"topology","outcome":"%s","observed_at":"%s","details":{"summary":"%s"}}\n' \
    "$assertion_id" "$outcome" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$details" \
    >>"$results_dir/topology/assertions.jsonl"
}

assert_equal() {
  assertion_id=$1
  expected=$2
  actual=$3
  details=$4
  if [ "$actual" = "$expected" ]; then
    record "$assertion_id" pass "$details"
  else
    record "$assertion_id" fail "$details"
    echo "$assertion_id failed: expected '$expected', got '$actual' ($details)" >&2
    return 1
  fi
}

container_networks() {
  service=$1
  container_id=$(compose ps --quiet "$service")
  docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}} {{end}}' "$container_id" |
    tr ' ' '\n' |
    sed '/^$/d' |
    sort |
    tr '\n' ' ' |
    sed 's/ $//'
}

published_port() {
  service=$1
  port=$2
  container_id=$(compose ps --quiet "$service")
  docker port "$container_id" "$port/tcp" 2>/dev/null || true
}

business_state() {
  compose exec -T postgres sh -eu -c \
    'export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql --host=postgres --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --tuples-only --no-align' <<'SQL' |
WITH snapshots(name, fingerprint) AS (
  SELECT 'runs', md5(coalesce((
    SELECT string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text)
    FROM favn_control.runs AS t
  ), ''))
  UNION ALL
  SELECT 'manifest_versions', md5(coalesce((
    SELECT string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text)
    FROM favn_control.manifest_versions AS t
  ), ''))
  UNION ALL
  SELECT 'backfills', md5(coalesce((
    SELECT string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text)
    FROM favn_control.backfills AS t
  ), ''))
  UNION ALL
  SELECT 'rebuild_operations', md5(coalesce((
    SELECT string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text)
    FROM favn_control.rebuild_operations AS t
  ), ''))
  UNION ALL
  SELECT 'target_recovery_operations', md5(coalesce((
    SELECT string_agg(row_to_json(t)::text, E'\n' ORDER BY row_to_json(t)::text)
    FROM favn_control.target_recovery_operations AS t
  ), ''))
)
SELECT string_agg(name || chr(58) || fingerprint, '|' ORDER BY name)
FROM snapshots;
SQL
    tr -d '[:space:]'
}

docker version >/dev/null
rm -rf "$results_dir"
mkdir -p "$results_dir/topology" "$secrets_dir"
chmod 0755 "$secrets_dir"
umask 077
secrets_mount_source=$secrets_dir
if command -v cygpath >/dev/null 2>&1; then
  secrets_mount_source=$(cygpath -w "$secrets_dir")
fi
MSYS_NO_PATHCONV=1 docker run --rm \
  --mount "type=bind,source=$secrets_mount_source,target=/secrets" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  sh -eu -c '
    umask 027
    head -c 48 /dev/urandom | base64 | tr -d "\n" >/secrets/admin-password
    chown 0:10001 /secrets/admin-password
    chmod 0440 /secrets/admin-password
  '

compose config --quiet
sh "$script_dir/verify-image-source.sh" "$env_file" "$source_state"
sh "$script_dir/ensure-image-builder.sh" "$image_builder_name"
build_images certificates postgres control-plane security-browser

compose up certificates
compose up --detach postgres
compose --profile operations run --rm database-bootstrap

if ! compose --profile security --profile proxy-security up --detach \
  control-plane proxy-header-receiver https-proxy; then
  compose ps --all >&2 || true
  compose logs --no-color control-plane >&2 || true
  exit 1
fi

bootstrap_expression='case FavnStoragePostgres.Release.admin_bootstrap(%{workspace_ids: ["elastic-simulation"], username: "security-admin", display_name: "Security qualification administrator", password: File.read!("/run/secrets/favn-security/admin-password") |> String.trim()}) do {:ok, _result} -> :ok; {:error, failure} -> IO.inspect(failure); System.halt(1) end'
compose exec -T control-plane //app/bin/favn_control_plane eval "$bootstrap_expression"

public_network="${project_name}_security-public-edge"
api_network="${project_name}_security-operator-api"
database_network="${project_name}_security-database"
runner_network="${project_name}_security-runner-control"
view_network="${project_name}_security-view-upstream"
instrumentation_network="${project_name}_security-proxy-instrumentation"

assert_equal TOPO-001 "$database_network $api_network $runner_network $view_network" \
  "$(container_networks control-plane)" "control plane has only its four explicit trust networks"
assert_equal TOPO-002 "$instrumentation_network $public_network $view_network" \
  "$(container_networks https-proxy)" \
  "reverse proxy is the only bridge between public edge, View, and test instrumentation"
assert_equal TOPO-003 "$database_network" \
  "$(container_networks postgres)" "PostgreSQL is isolated on the database network"
assert_equal TOPO-004 "" "$(published_port control-plane 4000)" \
  "View has no direct host publication"
assert_equal TOPO-005 "" "$(published_port control-plane 4101)" \
  "private API has no direct host publication"
assert_equal TOPO-008 "$instrumentation_network" \
  "$(container_networks proxy-header-receiver)" \
  "proxy-header capture helper is unreachable from the public edge"

state_before=$(business_state)

compose --profile proxy-security run --rm proxy-security
for assertion_id in TP-001 TP-002 TP-003 TP-004; do
  record "$assertion_id" pass "trusted proxy security probe passed"
done
compose --profile security --profile proxy-security run --rm security-api
compose --profile security --profile proxy-security run --rm security-browser

state_after=$(business_state)
assert_equal STATE-001 "$state_before" "$state_after" \
  "denied and malformed endpoint probes do not alter durable business state"

compose --profile security --profile proxy-security create security-browser security-api >/dev/null
browser_container=$(compose --profile security --profile proxy-security ps --all --quiet security-browser)
api_container=$(compose --profile security --profile proxy-security ps --all --quiet security-api)
assert_equal TOPO-006 "$public_network" \
  "$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$browser_container")" \
  "browser attacker has only public-edge reachability"
assert_equal TOPO-007 "$api_network" \
  "$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$api_container")" \
  "private API probe has only operator-API reachability"
assert_equal HARDEN-001 "true" \
  "$(docker inspect --format '{{json .HostConfig.ReadonlyRootfs}}' "$browser_container")" \
  "security probes use a read-only root filesystem"
assert_equal HARDEN-002 "[\"ALL\"]" \
  "$(docker inspect --format '{{json .HostConfig.CapDrop}}' "$browser_container")" \
  "browser probe drops all Linux capabilities"
assert_equal HARDEN-003 "true" \
  "$(docker inspect --format '{{json .HostConfig.ReadonlyRootfs}}' "$api_container")" \
  "API probe uses a read-only root filesystem"
assert_equal HARDEN-004 "[\"ALL\"]" \
  "$(docker inspect --format '{{json .HostConfig.CapDrop}}' "$api_container")" \
  "API probe drops all Linux capabilities"
assert_equal HARDEN-005 "[\"no-new-privileges:true\"]" \
  "$(docker inspect --format '{{json .HostConfig.SecurityOpt}}' "$browser_container")" \
  "browser probe cannot gain new privileges"
assert_equal HARDEN-006 "[\"no-new-privileges:true\"]" \
  "$(docker inspect --format '{{json .HostConfig.SecurityOpt}}' "$api_container")" \
  "API probe cannot gain new privileges"
assert_equal HARDEN-007 "" \
  "$(docker inspect --format '{{range .Mounts}}{{println .Destination}}{{end}}' "$browser_container" |
    grep -F '/var/run/docker.sock' || true)" \
  "browser probe has no Docker socket"
assert_equal HARDEN-008 "" \
  "$(docker inspect --format '{{range .Mounts}}{{println .Destination}}{{end}}' "$api_container" |
    grep -F '/var/run/docker.sock' || true)" \
  "API probe has no Docker socket"

compose --profile security --profile proxy-security rm --force --stop \
  security-browser security-api >/dev/null

compose --profile security --profile proxy-security run --rm \
  --workdir //results \
  --env FAVN_SECURITY_EVIDENCE_DIR=. \
  security-api node //security/verdict.mjs

passed_assertions=$(
  sed -n 's/.*"passed_assertions":\([0-9][0-9]*\).*/\1/p' "$results_dir/verdict.json"
)
echo "security qualification passed with $passed_assertions assertions ($source_state)"
echo "redacted evidence: $results_dir"
