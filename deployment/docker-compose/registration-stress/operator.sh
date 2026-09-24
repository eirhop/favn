#!/bin/sh
set -eu
# The build creates exactly one immutable publication directory.
set -- .favn/dist/manifest/*/manifest-index.json "${1:-help}"
[ "$#" -eq 2 ] && [ -f "$1" ] || { echo "expected one manifest publication" >&2; exit 1; }
manifest_path=$1
action=$2
manifest_directory=${manifest_path%/*}
manifest_id=${manifest_directory##*/}
case "$action" in
  publish) exec mix favn.publish --manifest "$manifest_path" ;;
  activate) exec mix favn.activate --workspace-id "$FAVN_WORKSPACE_ID" --manifest-version "$manifest_id" ;;
  run) exec mix favn.run CrmDemo.RegistrationStress.Pipeline --no-wait --refresh force_all ;;
  manifest-id) printf '%s\n' "$manifest_id" ;;
  *) echo "usage: operator {publish|activate|run|manifest-id}"; exit 64 ;;
esac
