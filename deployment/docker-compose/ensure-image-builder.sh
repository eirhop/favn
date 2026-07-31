#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
builder_name=${1:-favn-qualification-v1}

case "$builder_name" in
  ""|*[!a-z0-9_-]*)
    echo "refusing to use an invalid Docker builder name" >&2
    exit 1
    ;;
esac

if ! docker buildx inspect "$builder_name" >/dev/null 2>&1; then
  docker buildx create \
    --name "$builder_name" \
    --driver docker-container \
    --driver-opt default-load=true \
    --buildkitd-config "$script_dir/buildkitd.toml" >/dev/null
fi

inspection=$(docker buildx inspect --bootstrap "$builder_name")
driver=$(printf '%s\n' "$inspection" | sed -n 's/^Driver:[[:space:]]*//p' | head -n 1)
if [ "$driver" != "docker-container" ]; then
  echo "Docker builder $builder_name exists with unsupported driver $driver" >&2
  exit 1
fi

for expected in \
  "reservedSpace = '2GB'" \
  "maxUsedSpace = '12GB'" \
  "minFreeSpace = '20GB'"; do
  if ! printf '%s\n' "$inspection" | grep -Fq "$expected"; then
    echo "Docker builder $builder_name does not have the expected bounded GC policy" >&2
    echo "remove that builder explicitly, then rerun this command to recreate it" >&2
    exit 1
  fi
done
