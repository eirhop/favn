#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

mix credo --only warning --strict
mix dialyzer --format dialyzer --quiet-with-result
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
