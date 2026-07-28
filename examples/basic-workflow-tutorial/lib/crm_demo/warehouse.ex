defmodule CrmDemo.Warehouse do
  @moduledoc """
  Every SQL asset in this project writes to the same DuckDB connection.

  Namespace modules are structural: they hold defaults that descendants inherit
  root-to-leaf. Declaring the connection once here is the reason no leaf asset
  mentions it.
  """

  use Favn.Namespace

  relation(connection: :warehouse)
  materialized(:table)
  meta(owner: "crm-demo")
end
