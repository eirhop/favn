#!/usr/bin/env bash
set -euo pipefail

config=${1:-security/control-plane-grype.yaml}

if [[ ! -f $config ]]; then
  echo "Grype exception config does not exist: $config" >&2
  exit 1
fi

review_by=$(sed -n 's/^# review-by: //p' "$config")
if [[ ! $review_by =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Grype exception config must contain one '# review-by: YYYY-MM-DD' line" >&2
  exit 1
fi

if [[ $(date -u -d "$review_by" +%F) != "$review_by" ]]; then
  echo "Invalid Grype exception review date: $review_by" >&2
  exit 1
fi

today=$(date -u +%F)
if [[ $today > $review_by ]]; then
  echo "Grype exceptions expired on $review_by; review or remove every exception" >&2
  exit 1
fi

vulnerability_count=$(grep -c '^  - vulnerability:' "$config")
fix_state_count=$(grep -c '^    fix-state:' "$config")
package_count=$(grep -c '^    package:' "$config")
if [[ $vulnerability_count -eq 0 || $vulnerability_count -ne $fix_state_count || $vulnerability_count -ne $package_count ]]; then
  echo "Every Grype exception must include vulnerability, fix-state, and package constraints" >&2
  exit 1
fi

echo "Grype exceptions are review-valid through $review_by"
