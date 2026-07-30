#!/bin/sh
set -eu

: "${FAVN_COMPOSE_PROJECT_NAME:?FAVN_COMPOSE_PROJECT_NAME is required}"
: "${FAVN_SOURCE_REVISION:?FAVN_SOURCE_REVISION is required}"
: "${FAVN_IMAGE_TAG:?FAVN_IMAGE_TAG is required}"
: "${FAVN_RUNNER_RELEASE_ID:?FAVN_RUNNER_RELEASE_ID is required}"
: "${FAVN_PLATFORM_TOKEN:?FAVN_PLATFORM_TOKEN is required}"
: "${FAVN_CAPACITY_TOKEN:?FAVN_CAPACITY_TOKEN is required}"
: "${FAVN_RUNTIME_DATABASE_PASSWORD:?FAVN_RUNTIME_DATABASE_PASSWORD is required}"
: "${FAVN_QUALIFICATION_RUN_ID:?FAVN_QUALIFICATION_RUN_ID is required}"

api_url=${FAVN_API_URL:-http://control-plane:4101}
compose_file=${FAVN_COMPOSE_FILE:-/workspace/compose.yml}
env_file=${FAVN_COMPOSE_ENV_FILE:-/workspace/.env.local}
duration_seconds=${FAVN_QUALIFICATION_DURATION_SECONDS:-14400}
submit_interval_seconds=${FAVN_QUALIFICATION_SUBMIT_INTERVAL_SECONDS:-2}
sample_interval_seconds=${FAVN_QUALIFICATION_SAMPLE_INTERVAL_SECONDS:-10}
initial_backlog=${FAVN_QUALIFICATION_INITIAL_BACKLOG:-24}
drain_timeout_seconds=${FAVN_QUALIFICATION_DRAIN_TIMEOUT_SECONDS:-1800}
max_api_p95_ms=${FAVN_QUALIFICATION_MAX_API_P95_MS:-250}
max_queue_age_ms=${FAVN_QUALIFICATION_MAX_QUEUE_AGE_MS:-5000}
max_queue_depth=${FAVN_QUALIFICATION_MAX_QUEUE_DEPTH:-48}
min_submissions_per_minute=${FAVN_QUALIFICATION_MIN_SUBMISSIONS_PER_MINUTE:-4}
min_sample_coverage_percent=${FAVN_QUALIFICATION_MIN_SAMPLE_COVERAGE_PERCENT:-80}
runner_replacement_timeout_seconds=${FAVN_QUALIFICATION_RUNNER_REPLACEMENT_TIMEOUT_SECONDS:-120}
max_runners=${FAVN_SCALER_MAX_RUNNERS:-12}
max_launches=${FAVN_SCALER_MAX_LAUNCHES:-10000}
scaler_poll_seconds=${FAVN_SCALER_POLL_SECONDS:-1}
workspace_id=elastic-simulation
run_id=$FAVN_QUALIFICATION_RUN_ID
run_dir="/results/$run_id"
events_file="$run_dir/events.jsonl"
api_file="$run_dir/api-requests.jsonl"
submitted_file="$run_dir/submitted-runs.jsonl"
samples_file="$run_dir/samples.jsonl"
database_samples_file="$run_dir/database-samples.jsonl"
workload_outcomes_file="$run_dir/workload-outcomes.json"
docker_stats_file="$run_dir/docker-stats.jsonl"
performance_file="$run_dir/performance-summary.json"
runner_fault_file="$run_dir/runner-fault.json"
allowed_failures_file="$run_dir/allowed-runner-exits"
scaler_drain_signal_file="$run_dir/scaler-drain-requested"
final_file="$run_dir/final-validation.json"
status_file="$run_dir/status.json"
api_body_file="/tmp/favn-qualification-api-body"
api_headers_file="/tmp/favn-qualification-api-headers"
scaler_pid=
phase=initializing
failure_reason=
sequence=0

positive_integer() {
  name=$1
  value=$2

  case "$value" in
    ''|*[!0-9]*|0)
      echo "$name must be a positive integer" >&2
      exit 64
      ;;
  esac
}

positive_integer FAVN_QUALIFICATION_DURATION_SECONDS "$duration_seconds"
positive_integer FAVN_QUALIFICATION_SUBMIT_INTERVAL_SECONDS "$submit_interval_seconds"
positive_integer FAVN_QUALIFICATION_SAMPLE_INTERVAL_SECONDS "$sample_interval_seconds"
positive_integer FAVN_QUALIFICATION_INITIAL_BACKLOG "$initial_backlog"
positive_integer FAVN_QUALIFICATION_DRAIN_TIMEOUT_SECONDS "$drain_timeout_seconds"
positive_integer FAVN_QUALIFICATION_MAX_API_P95_MS "$max_api_p95_ms"
positive_integer FAVN_QUALIFICATION_MAX_QUEUE_AGE_MS "$max_queue_age_ms"
positive_integer FAVN_QUALIFICATION_MAX_QUEUE_DEPTH "$max_queue_depth"
positive_integer FAVN_QUALIFICATION_MIN_SUBMISSIONS_PER_MINUTE "$min_submissions_per_minute"
positive_integer FAVN_QUALIFICATION_MIN_SAMPLE_COVERAGE_PERCENT "$min_sample_coverage_percent"
positive_integer \
  FAVN_QUALIFICATION_RUNNER_REPLACEMENT_TIMEOUT_SECONDS \
  "$runner_replacement_timeout_seconds"
positive_integer FAVN_SCALER_MAX_RUNNERS "$max_runners"
positive_integer FAVN_SCALER_MAX_LAUNCHES "$max_launches"

if [ "$min_sample_coverage_percent" -gt 100 ]; then
  echo "FAVN_QUALIFICATION_MIN_SAMPLE_COVERAGE_PERCENT must be at most 100" >&2
  exit 64
fi

case "$run_id" in
  ''|*[!A-Za-z0-9._-]*)
    echo "FAVN_QUALIFICATION_RUN_ID contains unsupported characters" >&2
    exit 64
    ;;
esac

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

event() {
  event_name=$1
  detail=${2:-}

  jq -cn \
    --arg timestamp "$(timestamp)" \
    --arg phase "$phase" \
    --arg event "$event_name" \
    --arg detail "$detail" \
    '{timestamp:$timestamp,phase:$phase,event:$event,detail:$detail}' \
    >>"$events_file"
}

write_status() {
  state=$1

  jq -n \
    --arg run_id "$run_id" \
    --arg state "$state" \
    --arg phase "$phase" \
    --arg observed_at "$(timestamp)" \
    --arg failure_reason "$failure_reason" \
    '{
      run_id:$run_id,
      state:$state,
      phase:$phase,
      observed_at:$observed_at,
      failure_reason:(if $failure_reason == "" then null else $failure_reason end)
    }' >"$status_file"
}

fail() {
  failure_reason=$1
  event qualification_failed "$failure_reason"
  write_status failed
  echo "qualification failed: $failure_reason" >&2
  exit 1
}

cleanup() {
  exit_code=$?

  if [ -n "$scaler_pid" ] && kill -0 "$scaler_pid" 2>/dev/null; then
    kill "$scaler_pid" 2>/dev/null || true
    wait "$scaler_pid" 2>/dev/null || true
  fi

  if [ "$exit_code" -ne 0 ] && [ ! -f "$final_file" ]; then
    if [ -z "$failure_reason" ]; then
      failure_reason="qualification controller exited unexpectedly with code $exit_code"
    fi

    jq -n \
      --arg run_id "$run_id" \
      --arg completed_at "$(timestamp)" \
      --arg reason "$failure_reason" \
      '{
        run_id:$run_id,
        verdict:"failed",
        completed_at:$completed_at,
        reason:$reason,
        assertions:[]
      }' >"$final_file"
    write_status failed
  fi
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

compose() {
  docker compose \
    --env-file "$env_file" \
    --project-name "$FAVN_COMPOSE_PROJECT_NAME" \
    --file "$compose_file" \
    "$@"
}

service_container_id() {
  service=$1

  docker ps --all --quiet \
    --filter "label=com.docker.compose.project=$FAVN_COMPOSE_PROJECT_NAME" \
    --filter "label=com.docker.compose.service=$service" |
    head -n 1
}

running_runner_ids() {
  docker ps --quiet \
    --filter "label=com.docker.compose.project=$FAVN_COMPOSE_PROJECT_NAME" \
    --filter "label=com.docker.compose.service=runner"
}

runner_ids() {
  docker ps --all --quiet \
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

busy_runner_assignment() {
  PGPASSWORD="$FAVN_RUNTIME_DATABASE_PASSWORD" \
    PGCONNECT_TIMEOUT=5 \
    PGSSLROOTCERT=/etc/favn/postgres-tls/ca.crt \
    psql \
    --no-psqlrc \
    --quiet \
    --tuples-only \
    --no-align \
    --set "qualification_started_at=$qualification_started_at" \
    "host=postgres dbname=favn user=favn_runtime sslmode=verify-full" <<'SQL'
SELECT jsonb_build_object(
  'task_id', task_id,
  'runner_instance_id', assigned_runner_instance_id
)
FROM favn_control.runner_tasks
WHERE workspace_id = 'elastic-simulation'
  AND inserted_at >= :'qualification_started_at'::timestamptz
  AND status = 'running'
  AND assigned_runner_instance_id IS NOT NULL
ORDER BY updated_at DESC
LIMIT 1;
SQL
}

runner_instance_task_assignment() {
  runner_instance_id=$1

  PGPASSWORD="$FAVN_RUNTIME_DATABASE_PASSWORD" \
    PGCONNECT_TIMEOUT=5 \
    PGSSLROOTCERT=/etc/favn/postgres-tls/ca.crt \
    psql \
    --no-psqlrc \
    --quiet \
    --tuples-only \
    --no-align \
    --set "qualification_started_at=$qualification_started_at" \
    --set "runner_instance_id=$runner_instance_id" \
    "host=postgres dbname=favn user=favn_runtime sslmode=verify-full" <<'SQL'
SELECT jsonb_build_object(
  'task_id', task_id,
  'status', status
)
FROM favn_control.runner_tasks
WHERE workspace_id = 'elastic-simulation'
  AND inserted_at >= :'qualification_started_at'::timestamptz
  AND assigned_runner_instance_id = :'runner_instance_id'
ORDER BY updated_at DESC
LIMIT 1;
SQL
}

runner_demand() {
  curl --fail --silent --show-error \
    --max-time 5 \
    --header "Authorization: Bearer $FAVN_CAPACITY_TOKEN" \
    "$api_url/internal/runner-demand/default/$FAVN_RUNNER_RELEASE_ID" |
    jq -er '.outstanding'
}

wait_for_busy_runner() {
  deadline=$(( $(date +%s) + runner_replacement_timeout_seconds ))
  fault_work_submitted=false

  while [ "$(date +%s)" -lt "$deadline" ]; do
    if assignment=$(busy_runner_assignment) &&
        [ -n "$assignment" ] &&
        printf '%s' "$assignment" |
          jq -e '.task_id != null and .runner_instance_id != null' >/dev/null &&
        [ "$(running_runner_count)" -eq 1 ]; then
      busy_runner_name=$(printf '%s' "$assignment" | jq -r '.runner_instance_id')

      if [ "$(docker inspect --format '{{.State.Running}}' "$busy_runner_name" 2>/dev/null)" = "true" ]; then
        busy_runner_assignment_json=$assignment
        return 0
      fi
    fi

    if [ "$fault_work_submitted" = "false" ]; then
      submit_one runner_recovery "$runner_fault_target_id"
      fault_work_submitted=true
    fi

    sleep 1
  done

  return 1
}

request_id_from_headers() {
  sed -n 's/^[Xx]-[Rr]equest-[Ii][Dd]:[[:space:]]*//p' "$api_headers_file" |
    tr -d '\r' |
    tail -n 1
}

record_api() {
  operation=$1
  method=$2
  path=$3
  status=$4
  duration=$5
  transport=$6
  request_id=$(request_id_from_headers)

  jq -cn \
    --arg timestamp "$(timestamp)" \
    --arg phase "$phase" \
    --arg operation "$operation" \
    --arg method "$method" \
    --arg path "$path" \
    --arg status "$status" \
    --arg duration "$duration" \
    --arg request_id "$request_id" \
    --arg transport "$transport" \
    '{
      timestamp:$timestamp,
      phase:$phase,
      operation:$operation,
      method:$method,
      path:$path,
      status:(if $status == "" then null else ($status | tonumber) end),
      duration_seconds:(if $duration == "" then null else ($duration | tonumber) end),
      request_id:(if $request_id == "" then null else $request_id end),
      transport:$transport
    }' >>"$api_file"
}

api_get() {
  operation=$1
  path=$2
  : >"$api_body_file"
  : >"$api_headers_file"

  if metrics=$(
    curl --silent --show-error \
      --max-time 15 \
      --dump-header "$api_headers_file" \
      --output "$api_body_file" \
      --write-out '%{http_code} %{time_total}' \
      --header "Authorization: Bearer $FAVN_PLATFORM_TOKEN" \
      --header "X-Favn-Workspace-Id: $workspace_id" \
      "$api_url$path"
  ); then
    api_status=${metrics%% *}
    api_duration=${metrics#* }
    record_api "$operation" GET "$path" "$api_status" "$api_duration" ok
    return 0
  fi

  record_api "$operation" GET "$path" "" "" failed
  return 1
}

api_post() {
  operation=$1
  path=$2
  idempotency_key=$3
  payload=$4
  : >"$api_body_file"
  : >"$api_headers_file"

  if metrics=$(
    curl --silent --show-error \
      --max-time 30 \
      --dump-header "$api_headers_file" \
      --output "$api_body_file" \
      --write-out '%{http_code} %{time_total}' \
      --request POST \
      --header "Authorization: Bearer $FAVN_PLATFORM_TOKEN" \
      --header "X-Favn-Workspace-Id: $workspace_id" \
      --header "Idempotency-Key: $idempotency_key" \
      --header "Content-Type: application/json" \
      --data "$payload" \
      "$api_url$path"
  ); then
    api_status=${metrics%% *}
    api_duration=${metrics#* }
    record_api "$operation" POST "$path" "$api_status" "$api_duration" ok
    return 0
  fi

  record_api "$operation" POST "$path" "" "" failed
  return 1
}

wait_api_ready() {
  timeout_seconds=$1
  deadline=$(( $(date +%s) + timeout_seconds ))

  while [ "$(date +%s)" -lt "$deadline" ]; do
    if api_get readiness_recovery /api/orchestrator/v1/health/ready &&
        [ "$api_status" -eq 200 ] &&
        jq -e '.data.status == "ready"' "$api_body_file" >/dev/null; then
      return 0
    fi

    sleep 2
  done

  return 1
}

wait_container_healthy() {
  container_id=$1
  timeout_seconds=$2
  deadline=$(( $(date +%s) + timeout_seconds ))

  while [ "$(date +%s)" -lt "$deadline" ]; do
    running=$(docker inspect --format '{{.State.Running}}' "$container_id")

    if [ "$running" != "true" ]; then
      docker start "$container_id" >/dev/null 2>&1 || true
      sleep 2
      continue
    fi

    health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id")
    if [ "$health" = "healthy" ]; then
      return 0
    fi

    sleep 2
  done

  return 1
}

database_snapshot() {
  if ! database_json=$(
    PGPASSWORD="$FAVN_RUNTIME_DATABASE_PASSWORD" \
      PGCONNECT_TIMEOUT=5 \
      PGSSLROOTCERT=/etc/favn/postgres-tls/ca.crt \
      psql \
      --no-psqlrc \
      --quiet \
      --tuples-only \
      --no-align \
      --set "qualification_started_at=$qualification_started_at" \
      "host=postgres dbname=favn user=favn_runtime sslmode=verify-full" \
      --file /usr/local/share/favn/qualification-observe.sql
  ); then
    return 1
  fi

  printf '%s\n' "$database_json" | jq -c . >>"$database_samples_file"
}

collect_workload_outcomes() {
  if ! outcomes_json=$(
    PGPASSWORD="$FAVN_RUNTIME_DATABASE_PASSWORD" \
      PGCONNECT_TIMEOUT=5 \
      PGSSLROOTCERT=/etc/favn/postgres-tls/ca.crt \
      psql \
      --no-psqlrc \
      --quiet \
      --tuples-only \
      --no-align \
      --set "qualification_started_at=$qualification_started_at" \
      "host=postgres dbname=favn user=favn_runtime sslmode=verify-full" \
      --file /usr/local/share/favn/qualification-outcomes.sql
  ); then
    return 1
  fi

  printf '%s\n' "$outcomes_json" | jq . >"$workload_outcomes_file"
}

docker_snapshot() {
  observed_at=$(timestamp)
  container_ids=$(
    docker ps --quiet \
      --filter "label=com.docker.compose.project=$FAVN_COMPOSE_PROJECT_NAME"
  )

  for container_id in $container_ids; do
    docker stats --no-stream --format '{{json .}}' "$container_id" |
      jq -c \
        --arg observed_at "$observed_at" \
        --arg phase "$phase" \
        '. + {observed_at:$observed_at,phase:$phase}' \
        >>"$docker_stats_file"
  done
}

sample() {
  if ! api_get readiness_sample /api/orchestrator/v1/health/ready ||
      [ "$api_status" -ne 200 ]; then
    fail "readiness sample failed"
  fi
  readiness_json=$(cat "$api_body_file")

  if ! api_get submission_stats /api/orchestrator/v1/runs/submissions/stats ||
      [ "$api_status" -ne 200 ]; then
    fail "submission statistics sample failed"
  fi
  submission_json=$(cat "$api_body_file")

  if ! api_get in_flight /api/orchestrator/v1/runs/in-flight ||
      [ "$api_status" -ne 200 ]; then
    fail "in-flight run sample failed"
  fi
  in_flight_json=$(cat "$api_body_file")

  capacity_path="/api/orchestrator/v1/runner-capacity/default/$FAVN_RUNNER_RELEASE_ID"
  if ! api_get runner_capacity "$capacity_path" ||
      [ "$api_status" -ne 200 ]; then
    fail "runner capacity sample failed"
  fi
  capacity_json=$(cat "$api_body_file")
  runners=$(running_runner_count)

  jq -cn \
    --arg timestamp "$(timestamp)" \
    --arg phase "$phase" \
    --argjson readiness "$readiness_json" \
    --argjson submissions "$submission_json" \
    --argjson in_flight "$in_flight_json" \
    --argjson capacity "$capacity_json" \
    --argjson running_runners "$runners" \
    '{
      timestamp:$timestamp,
      phase:$phase,
      readiness:$readiness.data,
      submissions:$submissions.data.stats,
      in_flight:$in_flight.data,
      capacity:$capacity.data,
      running_runner_containers:$running_runners
    }' >>"$samples_file"

  if ! database_snapshot; then
    fail "read-only PostgreSQL statistics sample failed"
  fi

  docker_snapshot
  write_status running
}

run_payload() {
  payload_sequence=$1
  payload_phase=$2
  payload_variant=${3:-normal}
  payload_target_id=${4:-$target_id}

  jq -cn \
    --arg target_id "$payload_target_id" \
    --arg qualification_run_id "$run_id" \
    --arg phase "$payload_phase" \
    --arg variant "$payload_variant" \
    --argjson sequence "$payload_sequence" \
    '{
      target:{type:"asset",id:$target_id},
      refresh:"force_all",
      metadata:{
        qualification_run_id:$qualification_run_id,
        qualification_phase:$phase,
        qualification_sequence:$sequence,
        qualification_variant:$variant
      }
    }'
}

post_run_resolving_unknown() {
  operation=$1
  idempotency_key=$2
  payload=$3
  attempt=1

  while [ "$attempt" -le 12 ]; do
    if api_post "$operation" /api/orchestrator/v1/runs "$idempotency_key" "$payload"; then
      if [ "$api_status" -eq 202 ]; then
        return 0
      fi

      if [ "$api_status" -lt 500 ]; then
        return 0
      fi
    fi

    event mutation_outcome_resolution "replaying the exact request with its original idempotency key"
    wait_api_ready 120 || true
    sleep 2
    attempt=$(( attempt + 1 ))
  done

  return 1
}

record_accepted_run() {
  accepted_run_id=$1
  accepted_sequence=$2
  accepted_phase=$3

  jq -cn \
    --arg accepted_at "$(timestamp)" \
    --arg run_id "$accepted_run_id" \
    --arg phase "$accepted_phase" \
    --argjson sequence "$accepted_sequence" \
    '{accepted_at:$accepted_at,run_id:$run_id,phase:$phase,sequence:$sequence}' \
    >>"$submitted_file"
}

submit_one() {
  submit_phase=$1
  submit_target_id=${2:-$target_id}
  sequence=$(( sequence + 1 ))
  payload=$(run_payload "$sequence" "$submit_phase" normal "$submit_target_id")
  idempotency_key="$run_id-run-$sequence"

  if ! post_run_resolving_unknown run_submit "$idempotency_key" "$payload"; then
    fail "run submission outcome could not be resolved"
  fi

  if [ "$api_status" -ne 202 ]; then
    code=$(jq -r '.error.code // "unknown"' "$api_body_file")
    fail "run submission returned HTTP $api_status ($code)"
  fi

  accepted_run_id=$(jq -er '.data.run.id' "$api_body_file") ||
    fail "run submission response did not contain a run ID"
  record_accepted_run "$accepted_run_id" "$sequence" "$submit_phase"
}

prove_idempotency() {
  probe_sequence=0
  probe_key="$run_id-idempotency-probe"
  probe_payload=$(run_payload "$probe_sequence" idempotency original)

  if ! post_run_resolving_unknown idempotency_first "$probe_key" "$probe_payload" ||
      [ "$api_status" -ne 202 ]; then
    fail "initial idempotency probe was not accepted"
  fi
  first_run_id=$(jq -er '.data.run.id' "$api_body_file") ||
    fail "initial idempotency probe did not return a run ID"

  if ! api_post idempotency_replay /api/orchestrator/v1/runs "$probe_key" "$probe_payload" ||
      [ "$api_status" -ne 202 ]; then
    fail "identical idempotency replay was not accepted"
  fi
  replay_run_id=$(jq -er '.data.run.id' "$api_body_file") ||
    fail "idempotency replay did not return a run ID"

  if [ "$first_run_id" != "$replay_run_id" ]; then
    fail "identical idempotency replay returned a different run ID"
  fi

  conflict_payload=$(run_payload "$probe_sequence" idempotency conflicting)
  if ! api_post idempotency_conflict /api/orchestrator/v1/runs "$probe_key" "$conflict_payload"; then
    fail "idempotency conflict probe had a transport failure"
  fi

  if [ "$api_status" -ne 409 ] ||
      ! jq -e '.error.code == "idempotency_conflict"' "$api_body_file" >/dev/null; then
    fail "different content with the same idempotency key was not rejected"
  fi

  record_accepted_run "$first_run_id" "$probe_sequence" idempotency
  event idempotency_proved "identical replay returned the same run ID and changed content was rejected"
}

inject_runner_failure() {
  runner_fault_pause_started_epoch=$(date +%s)

  if ! wait_for_busy_runner; then
    fail "runner fault phase did not find a runner with an active task"
  fi

  killed_runner_name=$(printf '%s' "$busy_runner_assignment_json" | jq -r '.runner_instance_id')
  killed_task_id=$(printf '%s' "$busy_runner_assignment_json" | jq -r '.task_id')
  runner_id=$(docker inspect --format '{{.Id}}' "$killed_runner_name")
  baseline_runner_ids=$(runner_ids)
  fault_started_epoch=$(date +%s)

  printf '%s\n' "$runner_id" >>"$allowed_failures_file"
  event runner_crash_injected "$runner_id"
  docker kill --signal KILL "$runner_id" >/dev/null
  submit_one runner_recovery "$runner_fault_target_id"

  replacement_id=
  replacement_name=
  replacement_task_id=
  replacement_task_status=
  replacement_demand=0
  replacement_deadline=$(( $(date +%s) + runner_replacement_timeout_seconds ))

  while [ "$(date +%s)" -lt "$replacement_deadline" ]; do
    current_demand=$(runner_demand 2>/dev/null || printf '')

    case "$current_demand" in
      ''|*[!0-9]*)
        sleep 1
        continue
        ;;
    esac

    if [ "$current_demand" -gt "$replacement_demand" ]; then
      replacement_demand=$current_demand
    fi

    for candidate_id in $(runner_ids); do
      if printf '%s\n' "$baseline_runner_ids" | grep -F -x -q -- "$candidate_id"; then
        continue
      fi

      candidate_name=$(docker inspect --format '{{.Name}}' "$candidate_id" | sed 's#^/##')

      if assignment=$(runner_instance_task_assignment "$candidate_name") &&
          [ -n "$assignment" ] &&
          printf '%s' "$assignment" | jq -e '.task_id != null' >/dev/null; then
        replacement_id=$(docker inspect --format '{{.Id}}' "$candidate_id")
        replacement_name=$candidate_name
        replacement_task_id=$(printf '%s' "$assignment" | jq -r '.task_id')
        replacement_task_status=$(printf '%s' "$assignment" | jq -r '.status')
        break
      fi
    done

    if [ -n "$replacement_id" ] && [ "$replacement_demand" -gt 0 ]; then
      break
    fi

    sleep 1
  done

  if [ -z "$replacement_id" ] || [ "$replacement_demand" -le 0 ]; then
    fail "killed busy runner was not replaced by a distinct working runner after live demand"
  fi

  replacement_seconds=$(( $(date +%s) - fault_started_epoch ))

  jq -n \
    --arg killed_task_id "$killed_task_id" \
    --arg killed_runner_instance_id "$killed_runner_name" \
    --arg killed_container_id "$runner_id" \
    --arg replacement_runner_instance_id "$replacement_name" \
    --arg replacement_container_id "$replacement_id" \
    --arg replacement_task_id "$replacement_task_id" \
    --arg replacement_task_status "$replacement_task_status" \
    --argjson demand_observed_after_fault "$replacement_demand" \
    --argjson replacement_seconds "$replacement_seconds" \
    --argjson replacement_timeout_seconds "$runner_replacement_timeout_seconds" \
    '{
      killed_task_id:$killed_task_id,
      killed_runner_instance_id:$killed_runner_instance_id,
      killed_container_id:$killed_container_id,
      killed_runner_was_busy:true,
      replacement_runner_instance_id:$replacement_runner_instance_id,
      replacement_container_id:$replacement_container_id,
      replacement_claimed_work:true,
      replacement_task_id:$replacement_task_id,
      replacement_task_status:$replacement_task_status,
      demand_observed_after_fault:$demand_observed_after_fault,
      replacement_seconds:$replacement_seconds,
      replacement_timeout_seconds:$replacement_timeout_seconds
    }' >"$runner_fault_file"

  runner_fault_pause_seconds=$(( $(date +%s) - runner_fault_pause_started_epoch ))
  sampling_paused_seconds=$(( sampling_paused_seconds + runner_fault_pause_seconds ))
  event runner_replaced "$replacement_id"
}

inject_service_failure() {
  service=$1
  service_fault_pause_started_epoch=$(date +%s)
  container_id=$(service_container_id "$service")

  if [ -z "$container_id" ]; then
    fail "$service fault phase could not resolve its container"
  fi

  event "${service}_crash_injected" "$container_id"
  docker kill --signal KILL "$container_id" >/dev/null

  if ! wait_container_healthy "$container_id" 240; then
    fail "$service did not become healthy after crash recovery"
  fi

  if [ "$service" = "control-plane" ] && ! wait_api_ready 120; then
    fail "control plane API did not become ready after crash recovery"
  fi

  if [ "$service" = "postgres" ] && ! wait_api_ready 180; then
    fail "control plane did not recover after the PostgreSQL crash"
  fi

  service_fault_pause_seconds=$(( $(date +%s) - service_fault_pause_started_epoch ))
  sampling_paused_seconds=$(( sampling_paused_seconds + service_fault_pause_seconds ))
  event "${service}_recovered" "$container_id"
}

collect_logs_and_state() {
  controller_hostname=$(hostname)
  controller_id=$(docker inspect --format '{{.Id}}' "$controller_hostname")
  control_plane_id=$(service_container_id control-plane)
  postgres_id=$(service_container_id postgres)

  docker logs --timestamps "$controller_id" >"$run_dir/controller.log" 2>&1
  docker logs --timestamps "$control_plane_id" >"$run_dir/control-plane.log" 2>&1
  docker logs --timestamps "$postgres_id" >"$run_dir/postgres.log" 2>&1
  : >"$run_dir/runner.log"
  : >"$run_dir/container-states.jsonl"

  container_ids=$(
    docker ps --all --quiet \
      --filter "label=com.docker.compose.project=$FAVN_COMPOSE_PROJECT_NAME"
  )

  for container_id in $container_ids; do
    service=$(
      docker inspect \
        --format '{{index .Config.Labels "com.docker.compose.service"}}' \
        "$container_id"
    )
    name=$(docker inspect --format '{{.Name}}' "$container_id" | sed 's#^/##')
    state=$(docker inspect --format '{{json .State}}' "$container_id")

    jq -cn \
      --arg container_id "$container_id" \
      --arg name "$name" \
      --arg service "$service" \
      --argjson state "$state" \
      '{container_id:$container_id,name:$name,service:$service,state:$state}' \
      >>"$run_dir/container-states.jsonl"

    if [ "$service" = "runner" ]; then
      docker logs --timestamps "$container_id" >>"$run_dir/runner.log" 2>&1 || true
    fi
  done
}

api_p95_ms() {
  jq -r '
    select(
      .transport == "ok" and
      .status != null and
      .duration_seconds != null
    )
    | .duration_seconds * 1000
  ' "$api_file" |
    sort -n |
    awk '
      { values[NR] = $1 }
      END {
        if (NR == 0) {
          print 0
        } else {
          percentile_index = int((NR * 95 + 99) / 100)
          print values[percentile_index]
        }
      }
    '
}

collect_performance_summary() {
  observed_api_p95_ms=$(api_p95_ms)
  observed_max_queue_age_ms=$(
    jq -s '[.[] | (.submissions.oldest_queued_age_ms // 0)] | max // 0' "$samples_file"
  )
  observed_max_queue_depth=$(
    jq -s '[.[] | (.submissions.queued_depth // 0)] | max // 0' "$samples_file"
  )
  sustained_submissions=$(
    jq -s '
      [
        .[]
        | select(.phase == "sustained_load" or .phase == "runner_recovery")
      ]
      | length
    ' "$submitted_file"
  )
  sustained_samples=$(
    jq -s '
      [
        .[]
        | select(
            .phase == "sustained_load" or
            .phase == "runner_recovery" or
            .phase == "control_plane_recovery" or
            .phase == "postgres_recovery"
          )
      ]
      | length
    ' "$samples_file"
  )
  minimum_sustained_submissions=$(( (duration_seconds * min_submissions_per_minute + 59) / 60 ))
  sampling_eligible_seconds=$(( duration_seconds - sampling_paused_seconds ))

  if [ "$sampling_eligible_seconds" -lt "$sample_interval_seconds" ]; then
    sampling_eligible_seconds=$sample_interval_seconds
  fi

  expected_samples=$(( sampling_eligible_seconds / sample_interval_seconds ))

  if [ "$expected_samples" -lt 1 ]; then
    expected_samples=1
  fi

  minimum_samples=$(( (expected_samples * min_sample_coverage_percent + 99) / 100 ))

  jq -n \
    --argjson api_p95_ms "$observed_api_p95_ms" \
    --argjson max_api_p95_ms "$max_api_p95_ms" \
    --argjson max_queue_age_ms "$observed_max_queue_age_ms" \
    --argjson queue_age_budget_ms "$max_queue_age_ms" \
    --argjson max_queue_depth "$observed_max_queue_depth" \
    --argjson queue_depth_budget "$max_queue_depth" \
    --argjson sustained_submissions "$sustained_submissions" \
    --argjson minimum_sustained_submissions "$minimum_sustained_submissions" \
    --argjson sustained_samples "$sustained_samples" \
    --argjson sampling_paused_seconds "$sampling_paused_seconds" \
    --argjson sampling_eligible_seconds "$sampling_eligible_seconds" \
    --argjson expected_samples "$expected_samples" \
    --argjson minimum_samples "$minimum_samples" \
    --argjson minimum_sample_coverage_percent "$min_sample_coverage_percent" \
    '{
      api_latency:{
        p95_ms:$api_p95_ms,
        maximum_p95_ms:$max_api_p95_ms
      },
      queue_pressure:{
        maximum_age_ms:$max_queue_age_ms,
        maximum_age_budget_ms:$queue_age_budget_ms,
        maximum_depth:$max_queue_depth,
        maximum_depth_budget:$queue_depth_budget
      },
      throughput:{
        sustained_submissions:$sustained_submissions,
        minimum_sustained_submissions:$minimum_sustained_submissions
      },
      sample_completeness:{
        sustained_samples:$sustained_samples,
        measured_fault_pause_seconds:$sampling_paused_seconds,
        sampling_eligible_seconds:$sampling_eligible_seconds,
        expected_samples:$expected_samples,
        minimum_samples:$minimum_samples,
        minimum_coverage_percent:$minimum_sample_coverage_percent
      }
    }' >"$performance_file"
}

scan_evidence_for_secrets() {
  secret_scan_expected=8
  secret_scan_count=0
  secret_leak_names='[]'

  while IFS='=' read -r name value; do
    case "$name" in
      FAVN_MIGRATOR_DATABASE_PASSWORD|\
      FAVN_RUNTIME_DATABASE_PASSWORD|\
      FAVN_RUNTIME_INPUT_PIN_KEYS|\
      FAVN_PLATFORM_TOKEN|\
      FAVN_CAPACITY_TOKEN|\
      FAVN_BOOTSTRAP_PASSWORD|\
      FAVN_DISTRIBUTION_COOKIE|\
      FAVN_VIEW_SECRET_KEY_BASE)
        secret_scan_count=$(( secret_scan_count + 1 ))

        if [ "$name" = "FAVN_RUNTIME_INPUT_PIN_KEYS" ]; then
          secret_values=$(printf '%s' "$value" | jq -er '.[]')
        else
          secret_values=$value
        fi

        for secret in $secret_values; do
          if [ -n "$secret" ] && grep -R -F -q -- "$secret" "$run_dir"; then
            secret_leak_names=$(
              printf '%s' "$secret_leak_names" |
                jq -c --arg name "$name" '. + [$name] | unique'
            )
          fi
        done
        ;;
    esac
  done <"$env_file"
}

mkdir -p "$run_dir"
: >"$events_file"
: >"$api_file"
: >"$submitted_file"
: >"$samples_file"
: >"$database_samples_file"
: >"$workload_outcomes_file"
: >"$docker_stats_file"
: >"$allowed_failures_file"

qualification_started_at=$(timestamp)

jq -n \
  --arg run_id "$run_id" \
  --arg started_at "$qualification_started_at" \
  --arg source_revision "$FAVN_SOURCE_REVISION" \
  --arg image_tag "$FAVN_IMAGE_TAG" \
  --arg runner_release_id "$FAVN_RUNNER_RELEASE_ID" \
  --arg workspace_id "$workspace_id" \
  --argjson duration_seconds "$duration_seconds" \
  --argjson submit_interval_seconds "$submit_interval_seconds" \
  --argjson sample_interval_seconds "$sample_interval_seconds" \
  --argjson initial_backlog "$initial_backlog" \
  --argjson max_runners "$max_runners" \
  --argjson max_api_p95_ms "$max_api_p95_ms" \
  --argjson max_queue_age_ms "$max_queue_age_ms" \
  --argjson max_queue_depth "$max_queue_depth" \
  --argjson min_submissions_per_minute "$min_submissions_per_minute" \
  --argjson min_sample_coverage_percent "$min_sample_coverage_percent" \
  --argjson runner_replacement_timeout_seconds "$runner_replacement_timeout_seconds" \
  '{
    run_id:$run_id,
    started_at:$started_at,
    source_revision:$source_revision,
    image_tag:$image_tag,
    runner_release_id:$runner_release_id,
    workspace_id:$workspace_id,
    duration_seconds:$duration_seconds,
    submit_interval_seconds:$submit_interval_seconds,
    sample_interval_seconds:$sample_interval_seconds,
    initial_backlog:$initial_backlog,
    max_runners:$max_runners,
    acceptance_thresholds:{
      maximum_api_p95_ms:$max_api_p95_ms,
      maximum_queue_age_ms:$max_queue_age_ms,
      maximum_queue_depth:$max_queue_depth,
      minimum_submissions_per_minute:$min_submissions_per_minute,
      minimum_sample_coverage_percent:$min_sample_coverage_percent,
      runner_replacement_timeout_seconds:$runner_replacement_timeout_seconds
    },
    fault_schedule:["runner_crash_25_percent","control_plane_crash_50_percent","postgres_crash_75_percent"]
  }' >"$run_dir/run.json"

event qualification_started "local production-shaped PostgreSQL qualification"
write_status running

phase=target_resolution
if ! api_get active_manifest /api/orchestrator/v1/manifests/active ||
    [ "$api_status" -ne 200 ]; then
  fail "active manifest could not be read"
fi

target_id=$(
  jq -er '
    [
      .data.targets.assets[]
      | select(.asset_ref | contains("CrmDemo.Lifecycle.ElasticScaleProbe.Fast"))
      | .target_id
    ]
    | if length == 1 then .[0] else error("expected one fast probe target") end
  ' "$api_body_file"
) || fail "active manifest did not contain exactly one fast probe target"

runner_fault_target_id=$(
  jq -er '
    [
      .data.targets.assets[]
      | select(.asset_ref | contains("CrmDemo.Lifecycle.ElasticScaleProbe.Slow"))
      | .target_id
    ]
    | if length == 1 then .[0] else error("expected one slow probe target") end
  ' "$api_body_file"
) || fail "active manifest did not contain exactly one slow probe target"

phase=idempotency
prove_idempotency

phase=initial_backlog
backlog_index=0
while [ "$backlog_index" -lt "$initial_backlog" ]; do
  submit_one initial_backlog
  backlog_index=$(( backlog_index + 1 ))
done
event initial_backlog_created "$initial_backlog runs plus the idempotency probe"

export FAVN_SCALER_EXPECT_SEQUENCE=false
export FAVN_SCALER_MAX_RUNNERS="$max_runners"
export FAVN_SCALER_MAX_LAUNCHES="$max_launches"
export FAVN_SCALER_POLL_SECONDS="$scaler_poll_seconds"
export FAVN_SCALER_TIMEOUT_SECONDS=$(( duration_seconds + drain_timeout_seconds + 600 ))
export FAVN_SCALER_TIMELINE="$run_dir/scaler-timeline.jsonl"
export FAVN_SCALER_ALLOWED_FAILURES_FILE="$allowed_failures_file"
export FAVN_SCALER_DRAIN_SIGNAL_FILE="$scaler_drain_signal_file"

/usr/local/bin/scale-favn-runners >"$run_dir/scaler.log" 2>&1 &
scaler_pid=$!
event scaler_started "maximum $max_runners runners"

load_started_epoch=$(date +%s)
load_deadline=$(( load_started_epoch + duration_seconds ))
runner_fault_at=$(( load_started_epoch + duration_seconds / 4 ))
control_plane_fault_at=$(( load_started_epoch + duration_seconds / 2 ))
postgres_fault_at=$(( load_started_epoch + (duration_seconds * 3) / 4 ))
last_sample_at=0
runner_fault_done=false
control_plane_fault_done=false
postgres_fault_done=false
sampling_paused_seconds=0
phase=sustained_load

while [ "$(date +%s)" -lt "$load_deadline" ]; do
  now=$(date +%s)

  if ! kill -0 "$scaler_pid" 2>/dev/null; then
    if wait "$scaler_pid"; then
      fail "scaler drained before sustained load ended"
    else
      fail "scaler failed during sustained load"
    fi
  fi

  if [ "$last_sample_at" -eq 0 ] ||
      [ $(( now - last_sample_at )) -ge "$sample_interval_seconds" ]; then
    sample
    last_sample_at=$now
  fi

  if [ "$runner_fault_done" = "false" ] && [ "$now" -ge "$runner_fault_at" ]; then
    phase=runner_recovery
    inject_runner_failure
    runner_fault_done=true
    phase=sustained_load
  fi

  if [ "$control_plane_fault_done" = "false" ] &&
      [ "$now" -ge "$control_plane_fault_at" ]; then
    phase=control_plane_recovery
    inject_service_failure control-plane
    control_plane_fault_done=true
    sample
    last_sample_at=$(date +%s)
    phase=sustained_load
  fi

  if [ "$postgres_fault_done" = "false" ] && [ "$now" -ge "$postgres_fault_at" ]; then
    phase=postgres_recovery
    inject_service_failure postgres
    postgres_fault_done=true
    sample
    last_sample_at=$(date +%s)
    phase=sustained_load
  fi

  submit_one sustained_load
  sleep "$submit_interval_seconds"
done

if [ "$runner_fault_done" != "true" ] ||
    [ "$control_plane_fault_done" != "true" ] ||
    [ "$postgres_fault_done" != "true" ]; then
  fail "sustained load ended before every scheduled fault was injected"
fi

phase=draining
touch "$scaler_drain_signal_file"
event submissions_stopped "waiting for durable work and runners to drain"
write_status draining
drain_deadline=$(( $(date +%s) + drain_timeout_seconds ))

while kill -0 "$scaler_pid" 2>/dev/null &&
    [ "$(date +%s)" -lt "$drain_deadline" ]; do
  now=$(date +%s)
  if [ $(( now - last_sample_at )) -ge "$sample_interval_seconds" ]; then
    sample
    last_sample_at=$now
  fi
  sleep 2
done

if kill -0 "$scaler_pid" 2>/dev/null; then
  fail "scaler did not drain within $drain_timeout_seconds seconds"
fi

if ! wait "$scaler_pid"; then
  fail "scaler failed while draining"
fi
scaler_pid=
event scaler_drained "durable demand and runner containers returned to zero"

phase=final_validation
sample

if ! compose run --rm --no-deps database-verify >"$run_dir/schema-verification.log" 2>&1; then
  fail "final schema verification failed"
fi

if ! collect_workload_outcomes; then
  fail "final workload outcome collection failed"
fi

collect_logs_and_state
collect_performance_summary

if [ ! -s "$runner_fault_file" ]; then
  fail "runner replacement evidence is missing"
fi

final_sample=$(tail -n 1 "$samples_file")
final_database=$(tail -n 1 "$database_samples_file")
submitted_count=$(wc -l <"$submitted_file" | tr -d ' ')
performance_summary=$(cat "$performance_file")
runner_fault=$(cat "$runner_fault_file")

assertions=$(
  jq -cn \
    --argjson sample "$final_sample" \
    --argjson database "$final_database" \
    --argjson submitted_count "$submitted_count" \
    --argjson performance "$performance_summary" \
    --argjson runner_fault "$runner_fault" \
    '[
      {
        id:"readiness",
        passed:($sample.readiness.status == "ready"),
        observed:$sample.readiness.status,
        expected:"ready"
      },
      {
        id:"submission_queue_drained",
        passed:(
          $sample.submissions.queued_depth == 0 and
          $sample.submissions.active_depth == 0 and
          $sample.submissions.retrying_depth == 0 and
          $sample.submissions.cancellation_requested_depth == 0
        ),
        observed:{
          queued:$sample.submissions.queued_depth,
          active:$sample.submissions.active_depth,
          retrying:$sample.submissions.retrying_depth,
          cancelling:$sample.submissions.cancellation_requested_depth
        },
        expected:"all zero"
      },
      {
        id:"in_flight_runs_drained",
        passed:($sample.in_flight.count == 0),
        observed:$sample.in_flight.count,
        expected:0
      },
      {
        id:"runner_partition_drained",
        passed:(
          $sample.capacity.drained == true and
          $sample.capacity.outstanding == 0 and
          $sample.capacity.active_runs == 0 and
          $sample.capacity.pending_operations == 0 and
          $sample.capacity.durable_blockers == 0 and
          $sample.capacity.registered == 0 and
          $sample.running_runner_containers == 0
        ),
        observed:$sample.capacity + {running_runner_containers:$sample.running_runner_containers},
        expected:"drained with every count zero"
      },
      {
        id:"accepted_submission_count",
        passed:(
          ($database.submission_statuses.submitted // 0) == $submitted_count and
          (($database.submission_statuses | keys) - ["submitted"] | length) == 0
        ),
        observed:$database.submission_statuses,
        expected:{submitted:$submitted_count}
      },
      {
        id:"sustained_api_latency",
        passed:(
          $performance.api_latency.p95_ms <=
            $performance.api_latency.maximum_p95_ms
        ),
        observed:$performance.api_latency.p95_ms,
        expected:{
          maximum_p95_ms:$performance.api_latency.maximum_p95_ms
        }
      },
      {
        id:"sustained_queue_pressure",
        passed:(
          $performance.queue_pressure.maximum_age_ms <=
            $performance.queue_pressure.maximum_age_budget_ms and
          $performance.queue_pressure.maximum_depth <=
            $performance.queue_pressure.maximum_depth_budget
        ),
        observed:{
          maximum_age_ms:$performance.queue_pressure.maximum_age_ms,
          maximum_depth:$performance.queue_pressure.maximum_depth
        },
        expected:{
          maximum_age_ms:$performance.queue_pressure.maximum_age_budget_ms,
          maximum_depth:$performance.queue_pressure.maximum_depth_budget
        }
      },
      {
        id:"sustained_submission_rate",
        passed:(
          $performance.throughput.sustained_submissions >=
            $performance.throughput.minimum_sustained_submissions
        ),
        observed:$performance.throughput.sustained_submissions,
        expected:{
          minimum_sustained_submissions:
            $performance.throughput.minimum_sustained_submissions
        }
      },
      {
        id:"sustained_sample_completeness",
        passed:(
          $performance.sample_completeness.sustained_samples >=
            $performance.sample_completeness.minimum_samples
        ),
        observed:{
          sustained_samples:$performance.sample_completeness.sustained_samples,
          measured_fault_pause_seconds:
            $performance.sample_completeness.measured_fault_pause_seconds,
          sampling_eligible_seconds:
            $performance.sample_completeness.sampling_eligible_seconds,
          expected_samples:$performance.sample_completeness.expected_samples
        },
        expected:{
          minimum_samples:$performance.sample_completeness.minimum_samples,
          minimum_coverage_percent:
            $performance.sample_completeness.minimum_coverage_percent
        }
      },
      {
        id:"busy_runner_replaced",
        passed:(
          $runner_fault.killed_runner_was_busy == true and
          $runner_fault.replacement_claimed_work == true and
          $runner_fault.replacement_task_id != null and
          $runner_fault.replacement_task_id != "" and
          $runner_fault.demand_observed_after_fault > 0 and
          $runner_fault.killed_container_id !=
            $runner_fault.replacement_container_id and
          $runner_fault.replacement_seconds <=
            $runner_fault.replacement_timeout_seconds
        ),
        observed:$runner_fault,
        expected:{
          distinct_replacement:true,
          replacement_claimed_work:true,
          demand_observed_after_fault:"greater than zero",
          maximum_replacement_seconds:
            $runner_fault.replacement_timeout_seconds
        }
      },
      {
        id:"run_outcomes",
        passed:(
          $database.run_terminal == $submitted_count and
          $database.run_non_terminal == 0 and
          (($database.run_statuses | keys) - ["ok", "error"] | length) == 0 and
          ([($database.run_statuses | to_entries[] | .value)] | add // 0) ==
            $submitted_count and
          ($database.run_statuses.ok // 0) > 0 and
          ($database.run_statuses.error // 0) <= 3
        ),
        observed:{
          statuses:$database.run_statuses,
          terminal:$database.run_terminal,
          non_terminal:$database.run_non_terminal
        },
        expected:"all accepted runs terminal; only ok/error; at most one bounded error per injected fault"
      },
      {
        id:"runner_task_outcomes",
        passed:(
          $database.runner_task_terminal == $submitted_count and
          $database.runner_task_non_terminal == 0 and
          (($database.runner_task_statuses | keys) - ["succeeded", "unknown"] | length) == 0 and
          ([($database.runner_task_statuses | to_entries[] | .value)] | add // 0) ==
            $submitted_count and
          ($database.runner_task_statuses.unknown // 0) <= 1
        ),
        observed:{
          statuses:$database.runner_task_statuses,
          terminal:$database.runner_task_terminal,
          non_terminal:$database.runner_task_non_terminal
        },
        expected:"all tasks terminal; at most the deliberately killed runner has unknown outcome"
      },
      {
        id:"safe_failure_classification",
        passed:(
          (($database.run_failure_reasons | keys) -
            ["runner_outcome_unknown", "non_reusable_materialization_claim_succeeded"] |
            length) == 0 and
          ($database.run_failure_reasons.runner_outcome_unknown // 0) <= 1 and
          ($database.run_failure_reasons.non_reusable_materialization_claim_succeeded // 0) <= 2 and
          ([($database.run_failure_reasons | to_entries[] | .value)] | add // 0) ==
            ($database.run_statuses.error // 0)
        ),
        observed:$database.run_failure_reasons,
        expected:"only bounded unknown-runner or non-reusable-claim failures; never unsafe replay"
      },
      {
        id:"outbox_and_projection_drained",
        passed:(
          $database.outbox_unsequenced == 0 and
          $database.outbox_unpublished == 0 and
          $database.projection_lag == 0 and
          $database.projection_failures == 0
        ),
        observed:{
          outbox_unsequenced:$database.outbox_unsequenced,
          outbox_unpublished:$database.outbox_unpublished,
          projection_lag:$database.projection_lag,
          projection_failures:$database.projection_failures
        },
        expected:"all zero"
      },
      {
        id:"database_waits_drained",
        passed:($database.waiting_locks == 0 and $database.deadlocks == 0),
        observed:{waiting_locks:$database.waiting_locks,deadlocks:$database.deadlocks},
        expected:{waiting_locks:0,deadlocks:0}
      }
    ]'
)

scan_evidence_for_secrets

assertions=$(
  printf '%s' "$assertions" |
    jq -c \
      --argjson expected "$secret_scan_expected" \
      --argjson scanned "$secret_scan_count" \
      --argjson leaks "$secret_leak_names" \
      '. + [{
        id:"evidence_redaction",
        passed:($scanned == $expected and ($leaks | length) == 0),
        observed:{
          configured_secret_values_scanned:$scanned,
          leaked_variable_names:$leaks
        },
        expected:{
          configured_secret_values_scanned:$expected,
          leaked_variable_names:[]
        }
      }]'
)

if printf '%s' "$assertions" | jq -e 'all(.[]; .passed == true)' >/dev/null; then
  verdict=passed
else
  verdict=failed
  failure_reason="one or more final assertions failed"
fi

jq -n \
  --arg run_id "$run_id" \
  --arg verdict "$verdict" \
  --arg completed_at "$(timestamp)" \
  --arg source_revision "$FAVN_SOURCE_REVISION" \
  --argjson submitted_runs "$submitted_count" \
  --argjson assertions "$assertions" \
  '{
    run_id:$run_id,
    verdict:$verdict,
    completed_at:$completed_at,
    source_revision:$source_revision,
    submitted_runs:$submitted_runs,
    assertions:$assertions
  }' >"$final_file"

if [ "$verdict" != "passed" ]; then
  event qualification_failed "$failure_reason"
  write_status failed
  exit 1
fi

phase=complete
event qualification_passed "$submitted_count accepted runs reached terminal outcomes"
write_status passed
echo "qualification passed: $run_id ($submitted_count runs)"
