#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
output_path="$script_dir/.env.local"

if [ -e "$output_path" ] && [ "${1:-}" != "--force" ]; then
  echo "$output_path already exists. Use --force only to replace this disposable environment." >&2
  exit 1
fi

source_revision=${FAVN_SOURCE_REVISION:-$(git -C "$repository_root" rev-parse HEAD)}
if ! printf '%s\n' "$source_revision" | grep -Eq '^[0-9a-f]{40}$'; then
  echo "FAVN_SOURCE_REVISION must be a full lowercase Git revision" >&2
  exit 1
fi

short_revision=$(printf '%s' "$source_revision" | cut -c 1-12)
build_timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
temporary_path="$output_path.tmp"
umask 077
trap 'rm -f "$temporary_path"' EXIT HUP INT TERM

docker run --rm \
  --env FAVN_SOURCE_REVISION="$source_revision" \
  --env FAVN_SHORT_REVISION="$short_revision" \
  --env FAVN_BUILD_TIMESTAMP="$build_timestamp" \
  alpine:3.22 \
  sh -eu -c '
    random_hex() {
      head -c "$1" /dev/urandom | od -An -tx1 | tr -d " \n"
    }

    random_base64() {
      head -c "$1" /dev/urandom | base64 | tr -d "\n"
    }

    pin_key=$(random_base64 32)

    printf "%s\n" \
      "FAVN_COMPOSE_PROJECT_NAME=favn-elastic-simulation" \
      "FAVN_IMAGE_TAG=pr565-$FAVN_SHORT_REVISION" \
      "FAVN_SOURCE_REVISION=$FAVN_SOURCE_REVISION" \
      "FAVN_BUILD_TIMESTAMP=$FAVN_BUILD_TIMESTAMP" \
      "FAVN_RUNNER_RELEASE_ID=rr_$(random_hex 32)" \
      "FAVN_MIGRATOR_DATABASE_PASSWORD=$(random_hex 32)" \
      "FAVN_RUNTIME_DATABASE_PASSWORD=$(random_hex 32)" \
      "FAVN_RUNTIME_INPUT_PIN_KEYS={\"1\":\"$pin_key\"}" \
      "FAVN_PLATFORM_TOKEN=$(random_base64 48)" \
      "FAVN_CAPACITY_TOKEN=$(random_base64 48)" \
      "FAVN_DISTRIBUTION_COOKIE=$(random_base64 48)" \
      "FAVN_VIEW_SECRET_KEY_BASE=$(random_base64 64)" \
      "FAVN_VIEW_HOST_PORT=4173" \
      "FAVN_API_HOST_PORT=4101"
  ' >"$temporary_path"

mv "$temporary_path" "$output_path"
trap - EXIT HUP INT TERM
echo "created $output_path with disposable local credentials"
echo "the file is ignored by Git and must not be reused outside this drill"
