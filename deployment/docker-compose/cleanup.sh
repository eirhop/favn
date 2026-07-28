#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
compose_file="$script_dir/compose.yml"
env_file="$script_dir/.env.local"

if [ ! -f "$env_file" ]; then
  echo "$env_file does not exist; there is no resolved project to clean" >&2
  exit 1
fi

project_name=$(
  sed -n 's/^FAVN_COMPOSE_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)

case "$project_name" in
  ""|*[!a-z0-9_-]*)
    echo "refusing to clean an invalid Compose project name" >&2
    exit 1
    ;;
esac

docker compose \
  --env-file "$env_file" \
  --project-name "$project_name" \
  --file "$compose_file" \
  --profile operations \
  --profile runner \
  --profile scaler \
  down --volumes --remove-orphans

echo "removed the $project_name containers, network, PostgreSQL data, and certificate volumes"
