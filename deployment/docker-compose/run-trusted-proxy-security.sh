#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
compose_file="$script_dir/compose.yml"
env_file="$script_dir/.env.local"

if [ ! -f "$env_file" ]; then
  sh "$script_dir/prepare.sh"
fi

project_name=$(
  sed -n 's/^FAVN_COMPOSE_PROJECT_NAME=//p' "$env_file" |
    head -n 1
)

case "$project_name" in
  ""|*[!a-z0-9_-]*)
    echo "refusing to use an invalid Compose project name" >&2
    exit 1
    ;;
esac

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$project_name" \
    --file "$compose_file" \
    --profile proxy-security \
    "$@"
}

existing=$(compose ps --all --quiet)
if [ -n "$existing" ]; then
  echo "the $project_name project already has containers; inspect it or run cleanup.sh first" >&2
  exit 1
fi

docker version >/dev/null
mkdir -p "$script_dir/proxy-security-results"

compose pull https-proxy proxy-header-receiver proxy-security
compose build certificates postgres control-plane
compose up certificates
compose up --detach postgres

for operation in database-migrate database-grant workspace-provision database-verify; do
  compose run --rm "$operation"
done

compose up --detach control-plane proxy-header-receiver https-proxy
compose run --rm proxy-security

echo "trusted proxy security qualification passed"
