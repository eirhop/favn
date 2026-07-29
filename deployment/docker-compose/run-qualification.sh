#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
compose_file="$script_dir/compose.yml"
env_file="$script_dir/.env.local"
active_file="$script_dir/.qualification-active"

if [ -e "$active_file" ]; then
  echo "$active_file already exists; inspect it with qualification-status.sh or clean the project" >&2
  exit 1
fi

sh "$script_dir/run-simulation.sh"

project_name=$(
  sed -n 's/^FAVN_COMPOSE_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)
source_revision=$(
  sed -n 's/^FAVN_SOURCE_REVISION=//p' "$env_file" |
    head -n 1
)

if [ -z "$project_name" ] || [ -z "$source_revision" ]; then
  echo "the generated environment is missing its project name or source revision" >&2
  exit 1
fi

run_id=$(
  printf 'qual-%s-%s\n' \
    "$(date -u +'%Y%m%dT%H%M%SZ')" \
    "$(printf '%s' "$source_revision" | cut -c 1-12)"
)

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$project_name" \
    --file "$compose_file" \
    "$@"
}

mkdir -p "$script_dir/qualification-results"
controller_id=$(
  compose run \
    --detach \
    --no-deps \
    --env "FAVN_QUALIFICATION_RUN_ID=$run_id" \
    qualification |
    tail -n 1
)

case "$controller_id" in
  ''|*[!0-9a-f]*)
    echo "Docker did not return the qualification controller container ID" >&2
    exit 1
    ;;
esac

if [ "${#controller_id}" -ne 64 ]; then
  echo "Docker returned a non-canonical qualification controller container ID" >&2
  exit 1
fi

umask 077
printf '%s\n%s\n' "$run_id" "$controller_id" >"$active_file"

deadline=$(( $(date +%s) + 60 ))
status_path="$script_dir/qualification-results/$run_id/status.json"
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ -s "$status_path" ]; then
    break
  fi

  if [ "$(docker inspect --format '{{.State.Running}}' "$controller_id")" != "true" ]; then
    docker logs "$controller_id" >&2
    echo "qualification controller exited before writing status" >&2
    exit 1
  fi

  sleep 1
done

if [ ! -s "$status_path" ]; then
  echo "qualification controller did not write status within 60 seconds" >&2
  exit 1
fi

echo
echo "qualification started"
echo "run ID: $run_id"
echo "controller: $controller_id"
echo "evidence: $script_dir/qualification-results/$run_id"
echo "status: sh ./qualification-status.sh"
