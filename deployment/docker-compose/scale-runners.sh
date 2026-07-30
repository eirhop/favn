#!/bin/sh
set -eu

: "${FAVN_COMPOSE_PROJECT_NAME:?FAVN_COMPOSE_PROJECT_NAME is required}"
: "${FAVN_RUNNER_RELEASE_ID:?FAVN_RUNNER_RELEASE_ID is required}"
: "${FAVN_CAPACITY_TOKEN:?FAVN_CAPACITY_TOKEN is required}"

compose_file=${FAVN_COMPOSE_FILE:-/workspace/compose.yml}
env_file=${FAVN_COMPOSE_ENV_FILE:-/workspace/.env.local}
api_url=${FAVN_API_URL:-http://control-plane:4101}
max_runners=${FAVN_SCALER_MAX_RUNNERS:-3}
max_launches=${FAVN_SCALER_MAX_LAUNCHES:-16}
timeout_seconds=${FAVN_SCALER_TIMEOUT_SECONDS:-240}
poll_seconds=${FAVN_SCALER_POLL_SECONDS:-0.5}
expect_sequence=${FAVN_SCALER_EXPECT_SEQUENCE:-true}
timeline=${FAVN_SCALER_TIMELINE:-/results/simulation-timeline.jsonl}
allowed_failures_file=${FAVN_SCALER_ALLOWED_FAILURES_FILE:-}
drain_signal_file=${FAVN_SCALER_DRAIN_SIGNAL_FILE:-}

case "$max_runners" in
  ''|*[!0-9]*|0)
    echo "FAVN_SCALER_MAX_RUNNERS must be a positive integer" >&2
    exit 1
    ;;
esac

case "$max_launches" in
  ''|*[!0-9]*|0)
    echo "FAVN_SCALER_MAX_LAUNCHES must be a positive integer" >&2
    exit 1
    ;;
esac

case "$expect_sequence" in
  true|false) ;;
  *)
    echo "FAVN_SCALER_EXPECT_SEQUENCE must be true or false" >&2
    exit 1
    ;;
esac

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$FAVN_COMPOSE_PROJECT_NAME" \
    --file "$compose_file" \
    "$@"
}

is_container_id() {
  value=$1

  case "$value" in
    ''|*[!0-9a-f]*) return 1 ;;
  esac

  [ "${#value}" -eq 64 ]
}

running_runner_ids() {
  docker ps --quiet \
    --filter "label=com.docker.compose.project=$FAVN_COMPOSE_PROJECT_NAME" \
    --filter "label=com.docker.compose.service=runner"
}

running_runner_count() {
  ids=$(running_runner_ids)
  if [ -z "$ids" ]; then
    printf '0\n'
  else
    printf '%s\n' "$ids" | wc -l | tr -d ' '
  fi
}

sequence_state=0
last_sequence_count=
observe_sequence() {
  running=$1

  if [ "$expect_sequence" = "false" ] || [ "$running" = "$last_sequence_count" ]; then
    return
  fi

  last_sequence_count=$running

  case "$sequence_state:$running" in
    0:0) sequence_state=1 ;;
    1:3) sequence_state=2 ;;
    2:2) sequence_state=3 ;;
    3:1) sequence_state=4 ;;
    4:0) sequence_state=5 ;;
    *)
      echo "unexpected runner transition while verifying 0 -> 3 -> 2 -> 1 -> 0: $running" >&2
      exit 1
      ;;
  esac
}

snapshot() {
  phase=$1
  outstanding=$2
  running=$3

  jq -cn \
    --arg timestamp "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --arg phase "$phase" \
    --argjson outstanding "$outstanding" \
    --argjson running "$running" \
    '{timestamp:$timestamp,phase:$phase,outstanding:$outstanding,running:$running}' \
    >>"$timeline"

  printf '%s demand=%s runners=%s\n' "$phase" "$outstanding" "$running"
  observe_sequence "$running"
}

snapshot_api_unavailable() {
  running=$1

  jq -cn \
    --arg timestamp "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    --argjson running "$running" \
    '{timestamp:$timestamp,phase:"api_unavailable",outstanding:null,running:$running}' \
    >>"$timeline"

  printf 'api_unavailable demand=unknown runners=%s\n' "$running"
}

mkdir -p /results
mkdir -p "$(dirname -- "$timeline")"
: >"$timeline"

deadline=$(( $(date +%s) + timeout_seconds ))
seen_demand=false
launch_sequence=0
active_launched_ids=
scaler_instance="$(date +%s)-$$"

while [ "$(date +%s)" -lt "$deadline" ]; do
  still_running_ids=
  for container_id in $active_launched_ids; do
    running=$(docker inspect --format '{{.State.Running}}' "$container_id")

    if [ "$running" = "false" ]; then
      exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
      if [ "$exit_code" -ne 0 ]; then
        if [ -n "$allowed_failures_file" ] &&
            [ -f "$allowed_failures_file" ] &&
            grep -Fxq "$container_id" "$allowed_failures_file"; then
          :
        else
          echo "runner $container_id exited with code $exit_code" >&2
          exit 1
        fi
      fi
    else
      still_running_ids="$still_running_ids $container_id"
    fi
  done
  active_launched_ids=$still_running_ids

  if ! response=$(
      curl --fail --silent --show-error \
        --max-time 5 \
        --header "Authorization: Bearer $FAVN_CAPACITY_TOKEN" \
        "$api_url/internal/runner-demand/default/$FAVN_RUNNER_RELEASE_ID"
    ); then
    running=$(running_runner_count)
    snapshot_api_unavailable "$running"
    sleep "$poll_seconds"
    continue
  fi

  outstanding=$(printf '%s' "$response" | jq -er '.outstanding')
  running=$(running_runner_count)
  snapshot poll "$outstanding" "$running"

  if [ "$outstanding" -gt 0 ]; then
    seen_demand=true
  fi

  missing=$(( outstanding - running ))
  room=$(( max_runners - running ))
  if [ "$missing" -lt 0 ]; then
    missing=0
  fi
  if [ "$room" -lt 0 ]; then
    room=0
  fi
  if [ "$missing" -lt "$room" ]; then
    to_launch=$missing
  else
    to_launch=$room
  fi

  index=0
  while [ "$index" -lt "$to_launch" ]; do
    if [ "$launch_sequence" -ge "$max_launches" ]; then
      echo "runner launch limit $max_launches reached while demand remained outstanding" >&2
      exit 1
    fi

    launch_sequence=$(( launch_sequence + 1 ))
    container_name="$FAVN_COMPOSE_PROJECT_NAME-runner-$scaler_instance-$launch_sequence.favn.local"
    container_id=$(
      FAVN_RUNNER_NODE_HOST_ALIAS="$container_name" compose run \
        --detach \
        --no-deps \
        --name "$container_name" \
        --env "FAVN_RUNNER_INSTANCE_ID=$container_name" \
        --env "FAVN_RUNNER_NODE_HOST_ALIAS=$container_name" \
        runner |
        tail -n 1
    )

    if ! is_container_id "$container_id"; then
      echo "Docker did not return a runner container ID" >&2
      exit 1
    fi

    active_launched_ids="$active_launched_ids $container_id"

    index=$(( index + 1 ))
  done

  if [ "$to_launch" -gt 0 ]; then
    running=$(running_runner_count)
    snapshot launch "$outstanding" "$running"
  fi

  if [ "$seen_demand" = true ] && [ "$outstanding" -eq 0 ] && [ "$running" -eq 0 ]; then
    if [ -n "$drain_signal_file" ] && [ ! -f "$drain_signal_file" ]; then
      sleep "$poll_seconds"
      continue
    fi

    if [ "$expect_sequence" = true ] && [ "$sequence_state" -ne 5 ]; then
      echo "runner sequence did not contain 0 -> 3 -> 2 -> 1 -> 0" >&2
      exit 1
    fi

    if [ "$expect_sequence" = true ]; then
      echo "verified runner sequence 0 -> 3 -> 2 -> 1 -> 0"
    else
      echo "runner demand drained successfully"
    fi

    exit 0
  fi

  sleep "$poll_seconds"
done

echo "timed out waiting for demand and runners to drain" >&2
exit 1
