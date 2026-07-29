#!/bin/sh
set -eu

manifest_path=$(
  find .favn/dist/manifest -name manifest-index.json -type f |
    sort |
    head -n 1
)

if [ -z "$manifest_path" ]; then
  echo "built manifest was not found" >&2
  exit 1
fi

local_manifest_version_id=$(
  mix run --no-start -e '
    [path] = System.argv()
    {:ok, publication} = FavnAuthoring.Deployment.ManifestPublication.read(path)
    IO.puts(publication.version.manifest_version_id)
  ' -- "$manifest_path"
)
manifest_version_id=${FAVN_MANIFEST_VERSION_ID:-$local_manifest_version_id}

run_target() {
  exec mix favn.run "$1" \
    --refresh force_all \
    --wait-timeout-ms 180000 \
    --poll-interval-ms 500
}

case "${1:-help}" in
  publish)
    mix favn.publish --manifest "$manifest_path"
    ;;

  activate)
    mix favn.activate \
      --workspace-id "$FAVN_WORKSPACE_ID" \
      --manifest-version "$manifest_version_id"
    ;;

  run-fast)
    run_target CrmDemo.Lifecycle.ElasticScaleProbe.Fast
    ;;

  run-medium)
    run_target CrmDemo.Lifecycle.ElasticScaleProbe.Medium
    ;;

  run-slow)
    run_target CrmDemo.Lifecycle.ElasticScaleProbe.Slow
    ;;

  manifest-id)
    printf '%s\n' "$manifest_version_id"
    ;;

  help)
    echo "usage: favn-simulation-operator {publish|activate|run-fast|run-medium|run-slow|manifest-id}"
    ;;

  *)
    echo "unknown operation: $1" >&2
    exit 64
    ;;
esac
