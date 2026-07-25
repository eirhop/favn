defmodule FavnReferenceWorkload.SchemaVariant do
  @moduledoc """
  Compile-time helpers for the repeatable V1/V2 CRM lifecycle QA variants.

  This is intentionally example-only. It emits literal public DSL declarations
  so each compiled manifest has one ordinary, deterministic asset contract.
  """

  @doc false
  @spec schema_version!() :: :v1 | :v2
  def schema_version! do
    case System.get_env("CRM_EXAMPLE_SCHEMA_VERSION", "v1") do
      "v1" -> :v1
      "v2" -> :v2
      value -> raise "CRM_EXAMPLE_SCHEMA_VERSION must be v1 or v2, got: #{inspect(value)}"
    end
  end

  @doc "Emit the Source.Accounts contract and query for the selected example schema."
  defmacro accounts_definition do
    case schema_version!() do
      :v1 ->
        quote do
          contract do
            column(:account_id, :string, null: false)
            column(:name, :string, null: false)
            column(:segment, :string, null: false)
          end

          query do
            ~SQL"""
            select
              account_id,
              name,
              segment
            from read_json('.data/generic_crm/landing/accounts.json', auto_detect = true)
            """
          end
        end

      :v2 ->
        quote do
          contract do
            column(:account_id, :string, null: false)
            column(:name, :string, null: false)
            column(:segment, :string, null: false)
            column(:industry, :string, null: false)
          end

          query do
            ~SQL"""
            select
              account_id,
              name,
              segment,
              industry
            from read_json('.data/generic_crm/landing/accounts.json', auto_detect = true)
            """
          end
        end
    end
  end
end
