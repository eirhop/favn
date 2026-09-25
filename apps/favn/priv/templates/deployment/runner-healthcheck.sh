#!/bin/sh
set -eu

# This expiring snapshot is published by the running application, not by RPC.
read -r checked_at < "${FAVN_RUNNER_READINESS_FILE:?}" || exit 1
case "$checked_at" in
  ''|*[!0-9]*) exit 1 ;;
esac
[ "${#checked_at}" -eq 10 ] || exit 1
now=$(date +%s)
age=$((now - checked_at))
[ "$age" -ge 0 ] && [ "$age" -lt 10 ]
