#!/usr/bin/env bash
set -euo pipefail

missing_registry_reference() {
  local output=${1,,}
  [[ $output == *"manifest unknown"* || $output == *"name unknown"* || $output == *"not found"* ]]
}

lookup_digest() {
  local reference=$1 output digest status

  set +e
  output=$(docker buildx imagetools inspect "$reference" 2>&1)
  status=$?
  set -e

  if [[ $status -ne 0 ]]; then
    if missing_registry_reference "$output"; then
      return 3
    fi

    echo "registry lookup failed for $reference" >&2
    return 1
  fi

  digest=$(awk '$1 == "Digest:" {print $2; exit}' <<< "$output")

  if [[ ! $digest =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "registry response for $reference did not contain a valid digest" >&2
    return 1
  fi

  printf '%s\n' "$digest"
}

require_digest() {
  local reference=$1 expected=$2 actual status

  set +e
  actual=$(lookup_digest "$reference")
  status=$?
  set -e

  if [[ $status -eq 3 ]]; then
    echo "required registry reference is missing: $reference" >&2
    return 1
  fi

  [[ $status -eq 0 ]] || return "$status"

  if [[ $actual != "$expected" ]]; then
    echo "registry reference $reference points to $actual, expected $expected" >&2
    return 1
  fi
}

record_alias() {
  local source_reference=$1 alias_reference=$2 expected=$3 existing status recorded

  set +e
  existing=$(lookup_digest "$alias_reference")
  status=$?
  set -e

  if [[ $status -eq 0 ]]; then
    if [[ $existing != "$expected" ]]; then
      echo "refusing to overwrite $alias_reference ($existing != $expected)" >&2
      return 1
    fi

    echo "$alias_reference already points to $expected"
    return 0
  fi

  [[ $status -eq 3 ]] || return "$status"

  docker pull "$source_reference"
  docker tag "$source_reference" "$alias_reference"
  docker push "$alias_reference"
  recorded=$(lookup_digest "$alias_reference")

  if [[ $recorded != "$expected" ]]; then
    echo "registry did not record $alias_reference at $expected" >&2
    return 1
  fi
}

require_github_release_absent() {
  local repository=$1 tag=$2 output status http_status

  set +e
  output=$(gh api --include --method GET "repos/$repository/releases/tags/$tag" 2>&1)
  status=$?
  set -e

  http_status=$(awk '$1 ~ /^HTTP\// {value=$2} END {print value}' <<< "$output")

  if [[ $status -eq 0 && $http_status == 200 ]]; then
    echo "GitHub Release $tag already exists; tag promotion must publish it" >&2
    return 1
  fi

  if [[ $status -ne 0 && $http_status == 404 ]]; then
    return 0
  fi

  echo "GitHub Release lookup failed for $tag (HTTP ${http_status:-unknown})" >&2
  return 1
}

usage() {
  echo "usage: scripts/control_plane_registry.sh lookup-digest REF | require-digest REF DIGEST | record-alias SOURCE_REF ALIAS_REF DIGEST | require-github-release-absent REPOSITORY TAG" >&2
  exit 64
}

command=${1:-}
shift || true

case "$command" in
  lookup-digest)
    [[ $# -eq 1 ]] || usage
    lookup_digest "$1"
    ;;

  require-digest)
    [[ $# -eq 2 ]] || usage
    require_digest "$1" "$2"
    ;;

  record-alias)
    [[ $# -eq 3 ]] || usage
    record_alias "$1" "$2" "$3"
    ;;

  require-github-release-absent)
    [[ $# -eq 2 ]] || usage
    require_github_release_absent "$1" "$2"
    ;;

  *)
    usage
    ;;
esac
