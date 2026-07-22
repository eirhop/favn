#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: scripts/control_plane_release_qualification.sh IMAGE BUILD_ID DIGEST" >&2
  exit 64
fi

image=$1
build_id=$2
digest=$3

if [[ ! $digest =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "invalid control-plane image digest" >&2
  exit 64
fi

scripts/control_plane_image_contract.sh "$image" "$build_id" >/dev/null

repo_digest=$(docker image inspect --format '{{index .RepoDigests 0}}' "$image")
[[ $repo_digest == "ghcr.io/eirhop/favn-control-plane@$digest" ]]

echo "release qualification passed for $repo_digest"
