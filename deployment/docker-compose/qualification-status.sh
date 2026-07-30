#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
active_file="$script_dir/.qualification-active"

if [ ! -f "$active_file" ]; then
  echo "$active_file does not exist; no qualification run is registered" >&2
  exit 1
fi

run_id=$(sed -n '1p' "$active_file")
controller_id=$(sed -n '2p' "$active_file")

case "$run_id" in
  ''|*[!A-Za-z0-9._-]*)
    echo "the active qualification run ID is invalid" >&2
    exit 1
    ;;
esac

case "$controller_id" in
  ''|*[!0-9a-f]*)
    echo "the active qualification controller ID is invalid" >&2
    exit 1
    ;;
esac

if [ "${#controller_id}" -ne 64 ]; then
  echo "the active qualification controller ID is not canonical" >&2
  exit 1
fi

run_dir="$script_dir/qualification-results/$run_id"
mkdir -p "$run_dir"
docker logs --timestamps "$controller_id" >"$run_dir/controller.log" 2>&1

running=$(docker inspect --format '{{.State.Running}}' "$controller_id")
exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$controller_id")

echo "run ID: $run_id"
echo "controller: $controller_id"
echo "running: $running"
if [ "$running" = "false" ]; then
  echo "exit code: $exit_code"
fi
echo "evidence: $run_dir"

if [ -s "$run_dir/status.json" ]; then
  if command -v jq >/dev/null 2>&1; then
    jq . "$run_dir/status.json"
  else
    cat "$run_dir/status.json"
  fi
fi

if [ -s "$run_dir/final-validation.json" ]; then
  if command -v jq >/dev/null 2>&1; then
    jq . "$run_dir/final-validation.json"
  else
    cat "$run_dir/final-validation.json"
  fi
fi

if [ "$running" = "false" ] && [ "$exit_code" -ne 0 ]; then
  exit "$exit_code"
fi
