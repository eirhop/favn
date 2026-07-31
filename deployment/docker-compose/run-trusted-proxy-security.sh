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
    --profile proxy-security \
    "$@"
}

build_compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$image_build_project_name" \
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
sh "$script_dir/verify-image-source.sh" "$env_file" clean
sh "$script_dir/ensure-image-builder.sh" "$image_builder_name"
build_compose build --builder "$image_builder_name" --provenance=false \
  certificates postgres control-plane
compose up certificates
compose up --detach postgres

for operation in database-migrate database-grant workspace-provision database-verify; do
  compose run --rm "$operation"
done

compose up --detach control-plane proxy-header-receiver https-proxy
compose run --rm proxy-security

echo "trusted proxy security qualification passed"
