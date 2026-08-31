#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH='' cd -- "$script_dir/../.." && pwd)
compose_file="$script_dir/compose.yml"
memory_compose_file="$script_dir/compose.manifest-memory.yml"
env_file="$script_dir/.env.local"
results_dir="$script_dir/manifest-memory-results"
cycles=${FAVN_MANIFEST_MEMORY_CYCLES:-10}
settle_seconds=${FAVN_MANIFEST_MEMORY_SETTLE_SECONDS:-10}
expected_limit_bytes=${FAVN_MANIFEST_MEMORY_LIMIT_BYTES:-1073741824}

case "$cycles" in
  ''|*[!0-9]*) echo "FAVN_MANIFEST_MEMORY_CYCLES must be a positive integer" >&2; exit 64 ;;
  0) echo "FAVN_MANIFEST_MEMORY_CYCLES must be positive" >&2; exit 64 ;;
esac

if [ ! -f "$env_file" ]; then
  sh "$script_dir/prepare.sh"
fi

env_value() {
  sed -n "s/^$1=//p" "$env_file" | head -n 1
}

project_name=$(env_value FAVN_COMPOSE_PROJECT_NAME)
image_build_project_name=$(env_value FAVN_IMAGE_BUILD_PROJECT_NAME)
image_builder_name=$(env_value FAVN_IMAGE_BUILDER_NAME)

for name in "$project_name" "$image_build_project_name" "$image_builder_name"; do
  case "$name" in
    ''|*[!a-z0-9_-]*) echo "refusing to use an invalid Compose name" >&2; exit 1 ;;
  esac
done

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$project_name" \
    --file "$compose_file" \
    --file "$memory_compose_file" \
    "$@"
}

build_compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$image_build_project_name" \
    --file "$compose_file" \
    "$@"
}

client_curl() {
  client_id=$1
  shift

  docker exec "$client_id" /bin/sh -eu -c '
    exec curl --disable --noproxy "*" \
      --header "Authorization: Bearer $FAVN_PLATFORM_TOKEN" "$@"
  ' manifest-memory-curl "$@"
}

verify_manifest_http_auth() {
  client_id=$1

  deployer_status=$(client_curl "$client_id" --silent --show-error \
    --max-time 10 \
    --output /results/manifest-auth-preflight.json \
    --write-out '%{http_code}' \
    --request PUT \
    --header 'X-Favn-Workspace-Id: elastic-simulation' \
    http://control-plane:4101/api/orchestrator/v1/manifest-deployments/memory-auth-preflight || true)

  if [ "$deployer_status" != 422 ]; then
    echo "manifest HTTP authentication preflight returned $deployer_status" >&2
    return 1
  fi
}

wait_healthy() {
  service=$1
  container_id=$(compose ps --quiet "$service")
  deadline=$(( $(date +%s) + 180 ))

  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id")
    [ "$status" = healthy ] && return
    [ "$status" = unhealthy ] && break
    sleep 2
  done

  echo "$service did not become healthy" >&2
  exit 1
}

memory_value() {
  container_id=$1
  kind=$2

  docker exec "$container_id" /bin/sh -eu -c '
    kind=$1
    case "$kind" in
      current)
        if [ -r /sys/fs/cgroup/memory.current ]; then cat /sys/fs/cgroup/memory.current
        else cat /sys/fs/cgroup/memory/memory.usage_in_bytes; fi
        ;;
      peak)
        if [ -r /sys/fs/cgroup/memory.peak ]; then cat /sys/fs/cgroup/memory.peak
        else cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes; fi
        ;;
      limit)
        if [ -r /sys/fs/cgroup/memory.max ]; then cat /sys/fs/cgroup/memory.max
        else cat /sys/fs/cgroup/memory/memory.limit_in_bytes; fi
        ;;
      oom_kill)
        if [ -r /sys/fs/cgroup/memory.events ]; then
          grep "^oom_kill " /sys/fs/cgroup/memory.events | cut -d " " -f 2
        else
          grep "^oom_kill " /sys/fs/cgroup/memory/memory.oom_control 2>/dev/null | cut -d " " -f 2 || echo 0
        fi
        ;;
    esac
  ' measurement "$kind"
}

sample_memory() {
  container_id=$1
  output=$2

  while docker inspect --format '{{.State.Running}}' "$container_id" 2>/dev/null | grep -q true; do
    current=$(memory_value "$container_id" current 2>/dev/null || echo null)
    printf '{"timestamp_ms":%s,"current_bytes":%s}\n' "$(date +%s%3N)" "$current" >> "$output"
    sleep 0.2
  done
}

cleanup() {
  if [ -n "${sampler_pid:-}" ]; then
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
  fi
  compose --profile operations --profile runner down --timeout 10 --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

rm -rf "$results_dir"
mkdir -p "$results_dir"

docker version >/dev/null
command -v jq >/dev/null
sh "$script_dir/verify-image-source.sh" "$env_file" clean
sh "$script_dir/ensure-image-builder.sh" "$image_builder_name"

if build_compose build --help 2>/dev/null | grep -q -- '--provenance'; then
  build_compose build --builder "$image_builder_name" --provenance=false certificates postgres control-plane runner
else
  BUILDX_NO_DEFAULT_ATTESTATIONS=1 build_compose build --builder "$image_builder_name" certificates postgres control-plane runner
fi

initialize_environment() {
  compose --profile operations down --timeout 10 --volumes --remove-orphans >/dev/null 2>&1 || true
  compose up certificates
  compose up --detach postgres
  wait_healthy postgres
  compose run --rm database-bootstrap
}

await_terminal() {
  client_id=$1
  operation_id=$2
  output=$3
  output_name=$(basename "$output")
  deadline=$(( $(date +%s) + 600 ))

  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! client_curl "$client_id" --silent --show-error \
      --max-time 5 \
      --output "/results/$output_name" \
      --header 'X-Favn-Workspace-Id: elastic-simulation' \
      "http://control-plane:4101/api/orchestrator/v1/manifest-deployments/$operation_id"; then
      sleep 0.5
      continue
    fi
    state=$(jq --raw-output '.data.operation.state // empty' "$output" 2>/dev/null || true)

    case "$state" in
      succeeded|needs_attention) printf '%s\n' "$state"; return ;;
      failed|unknown) echo "manifest deployment $operation_id ended as $state" >&2; return 1 ;;
    esac
    sleep 0.5
  done

  echo "manifest deployment $operation_id did not become terminal" >&2
  return 1
}

run_fixture() {
  label=$1
  archive_path=$2
  archive_sha256=$3
  cycle=$4
  replay=${5:-no}
  operation_id="memory-${label}-${cycle}"
  samples="$results_dir/${operation_id}-samples.jsonl"
  response="$results_dir/${operation_id}-response.json"
  terminal_response="$results_dir/${operation_id}-terminal.json"
  archive_bytes=$(wc -c < "$archive_path" | tr -d ' ')

  initialize_environment
  compose up --detach control-plane
  wait_healthy control-plane
  compose --profile runner up --detach runner
  compose up --detach manifest-memory-client
  container_id=$(compose ps --quiet control-plane)
  client_id=$(compose ps --quiet manifest-memory-client)
  verify_manifest_http_auth "$client_id"
  limit_bytes=$(memory_value "$container_id" limit)
  if [ "$limit_bytes" != "$expected_limit_bytes" ]; then
    echo "control-plane cgroup limit is $limit_bytes bytes, expected $expected_limit_bytes" >&2
    return 1
  fi
  before_bytes=$(memory_value "$container_id" current)
  before_oom=$(memory_value "$container_id" oom_kill)

  sample_memory "$container_id" "$samples" &
  sampler_pid=$!
  deployment_started_at=$(date +%s)

  http_status=$(client_curl "$client_id" --silent --show-error \
    --max-time 900 \
    --output "/results/$(basename "$response")" \
    --write-out '%{http_code}' \
    --request PUT \
    --header 'X-Favn-Workspace-Id: elastic-simulation' \
    --header "X-Favn-Archive-Sha256: $archive_sha256" \
    --header 'Content-Type: application/gzip' \
    --upload-file "/archives/$(basename "$archive_path")" \
    "http://control-plane:4101/api/orchestrator/v1/manifest-deployments/$operation_id" || true)
  [ "$http_status" = 000 ] && http_status=0

  terminal_ok=false
  terminal_state=request_rejected
  if [ "$http_status" = 202 ]; then
    if terminal_state=$(await_terminal "$client_id" "$operation_id" "$terminal_response"); then
      terminal_ok=true
    else
      terminal_state=wait_failed
    fi
  else
    cp "$response" "$terminal_response" 2>/dev/null || true
  fi
  deployment_seconds=$(( $(date +%s) - deployment_started_at ))
  replay_status=null
  if [ "$replay" = yes ] && [ "$terminal_ok" = true ]; then
    replay_status=$(client_curl "$client_id" --silent --show-error \
      --max-time 30 \
      --output "/results/${operation_id}-replay.json" \
      --write-out '%{http_code}' \
      --request PUT \
      --header 'X-Favn-Workspace-Id: elastic-simulation' \
      --header "X-Favn-Archive-Sha256: $archive_sha256" \
      --header 'Content-Type: application/gzip' \
      --upload-file "/archives/$(basename "$archive_path")" \
      "http://control-plane:4101/api/orchestrator/v1/manifest-deployments/$operation_id" || true)
    [ "$replay_status" = 000 ] && replay_status=0
  fi

  sleep "$settle_seconds"
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  sampler_pid=

  running=$(docker inspect --format '{{.State.Running}}' "$container_id")
  oom_killed=$(docker inspect --format '{{.State.OOMKilled}}' "$container_id")
  restart_count=$(docker inspect --format '{{.RestartCount}}' "$container_id")
  if [ "$running" = true ]; then
    after_bytes=$(memory_value "$container_id" current)
    peak_bytes=$(memory_value "$container_id" peak)
    after_oom=$(memory_value "$container_id" oom_kill)
    health=$(docker inspect --format '{{.State.Health.Status}}' "$container_id")
  else
    after_bytes=null
    peak_bytes=null
    after_oom=null
    health=stopped
  fi

  printf '{"fixture":"%s","cycle":%s,"operation_id":"%s","archive_bytes":%s,"cgroup_limit_bytes":%s,"http_status":%s,"terminal_state":"%s","deployment_seconds":%s,"replay_status":%s,"before_bytes":%s,"after_bytes":%s,"peak_bytes":%s,"oom_kill_before":%s,"oom_kill_after":%s,"container_oom_killed":%s,"restart_count":%s,"health":"%s"}\n' \
    "$label" "$cycle" "$operation_id" "$archive_bytes" "$limit_bytes" "${http_status:-0}" "$terminal_state" "$deployment_seconds" "$replay_status" "$before_bytes" "$after_bytes" "$peak_bytes" "$before_oom" "$after_oom" "$oom_killed" "$restart_count" "$health" \
    >> "$results_dir/summary.jsonl"

  compose logs --no-color --timestamps control-plane > "$results_dir/${operation_id}-control-plane.log" 2>&1 || true

  if [ "$http_status" != 202 ] || [ "$terminal_ok" != true ] || [ "$deployment_seconds" -gt 900 ] || { [ "$replay" = yes ] && [ "$replay_status" != 200 ]; } || [ "$running" != true ] || [ "$oom_killed" != false ] || [ "$restart_count" != 0 ] || [ "$after_oom" != "$before_oom" ] || [ "$health" != healthy ]; then
    echo "manifest memory measurement failed for $operation_id" >&2
    return 1
  fi

  compose --profile operations --profile runner down --timeout 10 --volumes --remove-orphans >/dev/null
}

representative_archive="$repository_root/tmp/manifest-memory-90.tar.gz"
stress_archive="$repository_root/tmp/manifest-memory-1001.tar.gz"
representative_sha=$(sha256sum "$representative_archive" | cut -d ' ' -f 1)
stress_sha=$(sha256sum "$stress_archive" | cut -d ' ' -f 1)

cycle=1
while [ "$cycle" -le "$cycles" ]; do
  if [ "$cycle" -eq 1 ]; then replay=yes; else replay=no; fi
  run_fixture representative "$representative_archive" "$representative_sha" "$cycle" "$replay"
  cycle=$((cycle + 1))
done

run_fixture stress "$stress_archive" "$stress_sha" 1

echo "manifest import memory measurements written to $results_dir"
