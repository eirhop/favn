#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
compose_file="$script_dir/compose.yml"
env_file="$script_dir/.env.local"

if [ ! -f "$env_file" ]; then
  sh "$script_dir/prepare.sh"
fi

project_name=$(
  sed -n 's/^FAVN_COMPOSE_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)

if [ -z "$project_name" ]; then
  echo "FAVN_COMPOSE_PROJECT_NAME is missing from $env_file" >&2
  exit 1
fi

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$project_name" \
    --file "$compose_file" \
    "$@"
}

is_container_id() {
  value=$1

  case "$value" in
    ''|*[!0-9a-f]*) return 1 ;;
  esac

  [ "${#value}" -eq 64 ]
}

is_manifest_version_id() {
  value=$1
  digest=${value#mv_}

  case "$digest" in
    "$value"|''|*[!0-9a-f]*) return 1 ;;
  esac

  [ "${#digest}" -eq 32 ]
}

wait_healthy() {
  service=$1
  timeout_seconds=${2:-180}
  container_id=$(compose ps --quiet "$service")

  if [ -z "$container_id" ]; then
    echo "could not resolve the $service container" >&2
    exit 1
  fi

  deadline=$(( $(date +%s) + timeout_seconds ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(docker inspect --format '{{.State.Health.Status}}' "$container_id")

    case "$status" in
      healthy)
        echo "$service is healthy"
        return
        ;;
      unhealthy)
        echo "$service became unhealthy" >&2
        exit 1
        ;;
    esac

    sleep 2
  done

  echo "timed out waiting for $service health" >&2
  exit 1
}

existing=$(compose ps --all --quiet)
if [ -n "$existing" ]; then
  echo "the $project_name project already has containers; inspect it or run cleanup.sh first" >&2
  exit 1
fi

docker version >/dev/null
mkdir -p "$script_dir/simulation-results"

compose build certificates postgres control-plane operator runner scaler
compose up certificates
compose up --detach postgres
wait_healthy postgres

for operation in database-migrate database-grant workspace-provision database-verify; do
  compose run --rm "$operation"
done

compose up --detach control-plane
wait_healthy control-plane
publication_output=$(compose run --rm operator publish)
printf '%s\n' "$publication_output"
manifest_version_id=$(
  printf '%s\n' "$publication_output" |
    sed -n 's/^manifest version: //p' |
    tail -n 1
)

if ! is_manifest_version_id "$manifest_version_id"; then
  echo "publication did not return a canonical manifest version ID" >&2
  exit 1
fi

activation_id=$(
  compose run \
    --detach \
    --no-deps \
    --env "FAVN_MANIFEST_VERSION_ID=$manifest_version_id" \
    operator activate |
    tail -n 1
)

if ! is_container_id "$activation_id"; then
  echo "Docker did not return the activation container ID" >&2
  exit 1
fi

compose run \
  --rm \
  --no-deps \
  --env FAVN_SCALER_EXPECT_SEQUENCE=false \
  --env FAVN_SCALER_MAX_RUNNERS=1 \
  scaler

docker wait "$activation_id" >/dev/null
docker logs "$activation_id"
activation_exit=$(docker inspect --format '{{.State.ExitCode}}' "$activation_id")
docker rm "$activation_id" >/dev/null

if [ "$activation_exit" -ne 0 ]; then
  echo "manifest activation exited with code $activation_exit" >&2
  exit 1
fi

workload_ids=
for workload in fast medium slow; do
  workload_id=$(
    compose run \
      --detach \
      --no-deps \
      operator "run-$workload" |
      tail -n 1
  )

  if ! is_container_id "$workload_id"; then
    echo "Docker did not return the $workload operator container ID" >&2
    exit 1
  fi

  workload_ids="$workload_ids $workload_id"
done

compose up --detach scaler

for workload_id in $workload_ids; do
  docker wait "$workload_id" >/dev/null
  docker logs "$workload_id"
  workload_exit=$(docker inspect --format '{{.State.ExitCode}}' "$workload_id")
  docker rm "$workload_id" >/dev/null

  if [ "$workload_exit" -ne 0 ]; then
    echo "workload operator exited with code $workload_exit" >&2
    exit 1
  fi
done

scaler_id=$(compose ps --all --quiet scaler)
if [ -z "$scaler_id" ]; then
  echo "could not resolve the scaler container" >&2
  exit 1
fi

deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  running=$(docker inspect --format '{{.State.Running}}' "$scaler_id")
  if [ "$running" = "false" ]; then
    break
  fi
  sleep 1
done

running=$(docker inspect --format '{{.State.Running}}' "$scaler_id")
if [ "$running" != "false" ]; then
  echo "scaler did not exit within 120 seconds after the workloads completed" >&2
  exit 1
fi

compose logs scaler
scaler_exit=$(docker inspect --format '{{.State.ExitCode}}' "$scaler_id")
if [ "$scaler_exit" -ne 0 ]; then
  echo "scaler exited with code $scaler_exit" >&2
  exit 1
fi

echo
echo "simulation passed"
echo "PostgreSQL, the control plane, and runner evidence remain available for inspection"
echo "View: http://127.0.0.1:$(sed -n 's/^FAVN_VIEW_HOST_PORT=//p' "$env_file")"
echo "run: sh ./cleanup.sh"
