#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

run_quick_checks() {
  mix credo --only warning --strict
  mix sobelow \
    --root apps/favn_orchestrator \
    --router apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex \
    --private \
    --strict \
    --skip \
    --exit
  mix sobelow \
    --root apps/favn_view \
    --private \
    --strict \
    --skip \
    --exit
}

run_dialyzer() {
  mix dialyzer --format dialyzer --quiet-with-result
}

case "${1:-all}" in
  all)
    run_quick_checks
    run_dialyzer
    ;;
  quick)
    run_quick_checks
    ;;
  dialyzer)
    run_dialyzer
    ;;
  *)
    echo "usage: $0 [all|quick|dialyzer]" >&2
    exit 2
    ;;
esac
