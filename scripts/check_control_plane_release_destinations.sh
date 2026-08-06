#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 IMAGE_REPOSITORY RELEASE_TAG EXPECTED_DIGEST GITHUB_REPOSITORY" >&2
  exit 64
fi

image_repository=$1
release_tag=$2
expected_digest=$3
github_repository=$4

if [[ ! $release_tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?(\+[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
  echo "invalid release tag: $release_tag" >&2
  exit 64
fi

if [[ ! $expected_digest =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "invalid expected digest: $expected_digest" >&2
  exit 64
fi

expected_reference="$image_repository@$expected_digest"
registry_exists=false
release_exists=false
registry_error=$(mktemp)
release_error=$(mktemp)

cleanup() {
  rm -f "$registry_error" "$release_error"
}

trap cleanup EXIT

if registry_json=$(docker buildx imagetools inspect \
  "$image_repository:$release_tag" \
  --format '{{json .Manifest.Digest}}' \
  2>"$registry_error"); then
  if [[ $registry_json =~ ^\"(sha256:[0-9a-f]{64})\"$ ]]; then
    registry_digest=${BASH_REMATCH[1]}
  else
    echo "registry returned an invalid digest response" >&2
    exit 1
  fi

  if [[ $registry_digest != "$expected_digest" ]]; then
    echo "release image tag already points to unexpected digest: $registry_digest" >&2
    exit 1
  fi

  registry_exists=true
elif grep -Fqi -- "$image_repository:$release_tag: not found" "$registry_error" ||
  grep -Eqi '(manifest unknown|unexpected status[^[:cntrl:]]*404)' "$registry_error"; then
  :
else
  cat "$registry_error" >&2
  exit 1
fi

if release_body=$(gh api \
  "repos/$github_repository/releases/tags/$release_tag" \
  --jq .body \
  2>"$release_error"); then
  if [[ $release_body != *"$expected_reference"* ]]; then
    echo "existing GitHub release does not record the expected image digest" >&2
    exit 1
  fi

  release_exists=true
elif ! grep -Eq 'Not Found \(HTTP 404\)' "$release_error"; then
  cat "$release_error" >&2
  exit 1
fi

printf 'registry_exists=%s\n' "$registry_exists"
printf 'release_exists=%s\n' "$release_exists"
