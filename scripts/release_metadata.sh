#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_mode=${1:-values}

if [[ $output_mode != values && $output_mode != --export ]]; then
  echo "usage: scripts/release_metadata.sh [--export]" >&2
  exit 64
fi

one_value() {
  local label=$1 values=$2 count

  count=$(wc -w <<< "$values")
  if [[ $count -ne 1 ]]; then
    echo "expected one $label value, found $count" >&2
    exit 1
  fi

  printf '%s' "$values"
}

version=$(
  one_value \
    "release version" \
    "$(sed -n 's/^[[:space:]]*version: "\([^"]*\)",$/\1/p' "$repo_root/mix.exs")"
)

versioned_apps=(
  favn
  favn_authoring
  favn_azure
  favn_core
  favn_duckdb_adbc
  favn_local
  favn_orchestrator
  favn_runner
  favn_sql_runtime
  favn_storage_postgres
  favn_test_support
)

for app in "${versioned_apps[@]}"; do
  app_version=$(
    one_value \
      "$app version" \
      "$(sed -n 's/^[[:space:]]*version: "\([^"]*\)",$/\1/p' "$repo_root/apps/$app/mix.exs")"
  )

  if [[ $app_version != "$version" ]]; then
    echo "$app version $app_version does not match release version $version" >&2
    exit 1
  fi
done

manifest_schema=$(
  one_value \
    "manifest schema version" \
    "$(sed -n 's/^[[:space:]]*@manifest_schema_version \([0-9][0-9]*\)$/\1/p' \
      "$repo_root/apps/favn_core/lib/favn/manifest/contract_versions.ex")"
)
runner_contract=$(
  one_value \
    "runner contract version" \
    "$(sed -n 's/^[[:space:]]*@runner_contract_version \([0-9][0-9]*\)$/\1/p' \
      "$repo_root/apps/favn_core/lib/favn/manifest/contract_versions.ex")"
)

if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?(\+[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
  echo "invalid semantic release version: $version" >&2
  exit 1
fi

if [[ $output_mode == --export ]]; then
  printf 'export FAVN_CONTROL_PLANE_VERSION=%q\n' "$version"
  printf 'export FAVN_MANIFEST_SCHEMA_VERSION=%q\n' "$manifest_schema"
  printf 'export FAVN_RUNNER_CONTRACT_VERSION=%q\n' "$runner_contract"
else
  printf 'FAVN_CONTROL_PLANE_VERSION=%s\n' "$version"
  printf 'FAVN_MANIFEST_SCHEMA_VERSION=%s\n' "$manifest_schema"
  printf 'FAVN_RUNNER_CONTRACT_VERSION=%s\n' "$runner_contract"
fi
