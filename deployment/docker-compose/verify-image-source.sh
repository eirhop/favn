#!/bin/sh
set -eu

env_file=$1
expected_state=${2:-clean}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH='' cd -- "$script_dir/../.." && pwd)

source_revision=$(sed -n 's/^FAVN_SOURCE_REVISION=//p' "$env_file" | head -n 1)
image_tag=$(sed -n 's/^FAVN_IMAGE_TAG=//p' "$env_file" | head -n 1)
head_revision=$(git -C "$repository_root" rev-parse HEAD)
short_revision=$(printf '%s' "$head_revision" | cut -c 1-12)

if [ "$source_revision" != "$head_revision" ]; then
  echo "prepared image revision does not match the checked-out HEAD" >&2
  exit 1
fi

dirty=0
if [ -n "$(git -C "$repository_root" status --porcelain --untracked-files=normal)" ]; then
  dirty=1
fi

case "$expected_state" in
  clean)
    if [ "$dirty" = "1" ] || [ "$image_tag" != "pr565-$short_revision" ]; then
      echo "qualification images require the unchanged clean revision prepared in $env_file" >&2
      exit 1
    fi
    ;;
  dirty_diagnostic)
    if [ "$dirty" != "1" ] || ! printf '%s\n' "$image_tag" |
        grep -Eq "^diagnostic-$short_revision-[0-9]{14}-[0-9]+$"; then
      echo "dirty qualification requires a unique diagnostic image tag" >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported qualification source state $expected_state" >&2
    exit 1
    ;;
esac
