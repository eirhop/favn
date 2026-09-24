#!/bin/sh
set -eu
export HOME=/duckdb-home
case "${1:-init}" in
  init)
    for catalog in source core mart; do
      duckdb :memory: -batch -c "ATTACH '$FAVN_STRESS_CATALOG' AS $catalog (METADATA_SCHEMA 'stress_$catalog', DATA_PATH '/var/lib/favn/stress/$catalog'); CREATE SCHEMA IF NOT EXISTS $catalog.stress;" >/dev/null
    done
    echo 'Shared DuckLake catalogs initialized serially.'
    ;;
  audit)
    statement="ATTACH '$FAVN_STRESS_CATALOG' AS source (METADATA_SCHEMA 'stress_source', DATA_PATH '/var/lib/favn/stress/source');"
    for number in $(seq 1 35); do
      target=$(printf 'target_%02d' "$number")
      statement="$statement SELECT '$target' AS target, count(*) AS rows, count(DISTINCT id) AS distinct_ids, sum(id) AS sum_ids FROM source.stress.$target;"
    done
    duckdb :memory: -batch -json -c "$statement"
    ;;
  *) exit 64 ;;
esac
