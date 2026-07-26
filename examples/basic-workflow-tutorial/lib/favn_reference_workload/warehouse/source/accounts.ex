defmodule FavnReferenceWorkload.Warehouse.Source.Accounts do
  @moduledoc "Full-refresh DuckDB source table loaded from landing accounts JSON."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.{SchemaVariant, Warehouse.Landing.WriteExtracts}

  require SchemaVariant

  relation(true)
  execution_pool(:local_duckdb)
  depends({WriteExtracts, :accounts_snapshot})
  materialized(:table)
  meta(category: :accounts, tags: [:source, :full_refresh])

  SchemaVariant.accounts_definition()
end
