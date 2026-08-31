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
head_revision=$(git -C "$repository_root" rev-parse HEAD)
if [ "$source_revision" != "$head_revision" ]; then
  echo "FAVN_SOURCE_REVISION must identify the checked-out HEAD" >&2
  exit 1
fi

short_revision=$(printf '%s' "$source_revision" | cut -c 1-12)
image_tag=${FAVN_IMAGE_TAG:-pr565-$short_revision}
if [ -n "$(git -C "$repository_root" status --porcelain --untracked-files=normal)" ]; then
  if [ "${FAVN_QUALIFICATION_ALLOW_DIRTY:-0}" != "1" ]; then
    echo "Docker qualification requires a clean checkout" >&2
    exit 1
  fi
  if [ -z "${FAVN_IMAGE_TAG:-}" ] || ! printf '%s\n' "$image_tag" |
      grep -Eq "^diagnostic-$short_revision-[0-9]{14}-[0-9]+$"; then
    echo "dirty diagnostic builds require an explicit unique FAVN_IMAGE_TAG" >&2
    exit 1
  fi
elif [ "$image_tag" != "pr565-$short_revision" ]; then
  echo "clean qualification builds require the revision-addressed image tag" >&2
  exit 1
fi
if ! printf '%s\n' "$image_tag" | grep -Eq '^[a-z0-9][a-z0-9_.-]{0,127}$'; then
  echo "FAVN_IMAGE_TAG must be a valid lowercase Docker tag" >&2
  exit 1
fi

build_timestamp=${FAVN_BUILD_TIMESTAMP:-$(git -C "$repository_root" show -s --format=%cI "$source_revision")}
release_metadata=$("$repository_root/scripts/release_metadata.sh")
control_plane_version=$(printf '%s\n' "$release_metadata" | sed -n 's/^FAVN_CONTROL_PLANE_VERSION=//p')
manifest_schema_version=$(printf '%s\n' "$release_metadata" | sed -n 's/^FAVN_MANIFEST_SCHEMA_VERSION=//p')
runner_contract_version=$(printf '%s\n' "$release_metadata" | sed -n 's/^FAVN_RUNNER_CONTRACT_VERSION=//p')
for value in "$control_plane_version" "$manifest_schema_version" "$runner_contract_version"; do
  if [ -z "$value" ]; then
    echo "release metadata is incomplete" >&2
    exit 1
  fi
done
temporary_path="$output_path.tmp"
umask 077
trap 'rm -f "$temporary_path"' EXIT HUP INT TERM

docker run --rm \
  --env FAVN_COMPOSE_PROJECT_NAME="${FAVN_COMPOSE_PROJECT_NAME:-favn-elastic-simulation}" \
  --env FAVN_IMAGE_BUILD_PROJECT_NAME="${FAVN_IMAGE_BUILD_PROJECT_NAME:-favn-qualification-images}" \
  --env FAVN_IMAGE_BUILDER_NAME="${FAVN_IMAGE_BUILDER_NAME:-favn-qualification-v1}" \
  --env FAVN_IMAGE_TAG="$image_tag" \
  --env FAVN_SOURCE_REVISION="$source_revision" \
  --env FAVN_SHORT_REVISION="$short_revision" \
  --env FAVN_BUILD_TIMESTAMP="$build_timestamp" \
  --env FAVN_CONTROL_PLANE_VERSION="$control_plane_version" \
  --env FAVN_MANIFEST_SCHEMA_VERSION="$manifest_schema_version" \
  --env FAVN_RUNNER_CONTRACT_VERSION="$runner_contract_version" \
  alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce \
  sh -eu -c '
    random_hex() {
      head -c "$1" /dev/urandom | od -An -tx1 | tr -d " \n"
    }

    random_base64() {
      head -c "$1" /dev/urandom | base64 | tr -d "\n"
    }

    pin_key=$(random_base64 32)
    runner_release_id="rr_$(printf "%s" "favn-compose-runner:$FAVN_SOURCE_REVISION:$FAVN_IMAGE_TAG" | sha256sum | cut -d " " -f 1)"

    printf "%s\n" \
      "FAVN_COMPOSE_PROJECT_NAME=$FAVN_COMPOSE_PROJECT_NAME" \
      "FAVN_IMAGE_BUILD_PROJECT_NAME=$FAVN_IMAGE_BUILD_PROJECT_NAME" \
      "FAVN_IMAGE_BUILDER_NAME=$FAVN_IMAGE_BUILDER_NAME" \
      "FAVN_IMAGE_TAG=$FAVN_IMAGE_TAG" \
      "FAVN_SOURCE_REVISION=$FAVN_SOURCE_REVISION" \
      "FAVN_BUILD_TIMESTAMP=$FAVN_BUILD_TIMESTAMP" \
      "FAVN_CONTROL_PLANE_VERSION=$FAVN_CONTROL_PLANE_VERSION" \
      "FAVN_MANIFEST_SCHEMA_VERSION=$FAVN_MANIFEST_SCHEMA_VERSION" \
      "FAVN_RUNNER_CONTRACT_VERSION=$FAVN_RUNNER_CONTRACT_VERSION" \
      "FAVN_RUNNER_RELEASE_ID=$runner_release_id" \
      "FAVN_BOOTSTRAP_DATABASE_PASSWORD=$(random_hex 32)" \
      "FAVN_MIGRATOR_DATABASE_PASSWORD=$(random_hex 32)" \
      "FAVN_RUNTIME_DATABASE_PASSWORD=$(random_hex 32)" \
      "FAVN_RUNTIME_INPUT_PIN_KEYS={\"1\":\"$pin_key\"}" \
      "FAVN_PLATFORM_TOKEN=$(random_hex 48)" \
      "FAVN_MANIFEST_DEPLOYER_TOKEN=$(random_hex 48)" \
      "FAVN_CAPACITY_TOKEN=$(random_hex 48)" \
      "FAVN_DISTRIBUTION_COOKIE=$(random_hex 48)" \
      "FAVN_OPERATOR_COMMAND_HMAC_SECRET=$(random_hex 48)" \
      "FAVN_VIEW_SECRET_KEY_BASE=$(random_base64 64)" \
      "FAVN_VIEW_HOST_PORT=4173" \
      "FAVN_PROXY_HOST_PORT=4443" \
      "FAVN_API_HOST_PORT=4101"
  ' >"$temporary_path"

mv "$temporary_path" "$output_path"
trap - EXIT HUP INT TERM
echo "created $output_path with disposable local credentials"
echo "the file is ignored by Git and must not be reused outside this drill"
