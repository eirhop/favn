defmodule FavnSQLRuntime.SQLConcurrencyPolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Error

  defmodule AdapterWithPolicy do
    def default_concurrency_policy(%Resolved{} = resolved) do
      %ConcurrencyPolicy{ConcurrencyPolicy.single_writer(resolved) | applies_to: :all}
    end
  end

  defmodule AdapterWithCatalogPolicies do
    def concurrency_policies(%Resolved{} = resolved) do
      {:ok,
       [
         ConcurrencyPolicy.unlimited(resolved),
         ConcurrencyPolicy.catalog(resolved, "raw", :unlimited),
         ConcurrencyPolicy.catalog(resolved, "mart", 1)
       ]}
    end
  end

  test "loads adapter before checking default concurrency policy callback" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: AdapterWithPolicy,
      module: __MODULE__,
      config: %{}
    }

    assert {:ok,
            %ConcurrencyPolicy{
              limit: 1,
              applies_to: :all,
              admission_timeout_ms: 30_000,
              connection: :warehouse,
              scope: {:connection, :warehouse},
              target: :default
            }} =
             ConcurrencyPolicy.resolve(resolved)
  end

  test "loads configured admission timeout" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: AdapterWithPolicy,
      module: __MODULE__,
      config: %{admission_timeout_ms: 25}
    }

    assert {:ok, %ConcurrencyPolicy{admission_timeout_ms: 25}} =
              ConcurrencyPolicy.resolve(resolved)
  end

  test "loads adapter-provided catalog concurrency policies" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: AdapterWithCatalogPolicies,
      module: __MODULE__,
      config: %{admission_timeout_ms: 25}
    }

    assert {:ok,
            %Favn.SQL.ConcurrencyPolicies{
              default: %ConcurrencyPolicy{limit: :unlimited, target: :default},
              catalog: %{
                "raw" => %ConcurrencyPolicy{limit: :unlimited, scope: {:warehouse, "raw"}},
                "mart" => %ConcurrencyPolicy{limit: 1, scope: {:warehouse, "mart"}}
              }
            }} = ConcurrencyPolicy.resolve(resolved)
  end

  test "returns a config error for invalid admission timeout" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: AdapterWithPolicy,
      module: __MODULE__,
      config: %{admission_timeout_ms: 0}
    }

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{admission_timeout_ms: 0}
            }} = ConcurrencyPolicy.resolve(resolved)
  end

  test "returns a config error when adapter cannot be loaded" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: Missing.PolicyAdapter,
      module: __MODULE__,
      config: %{}
    }

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{adapter: Missing.PolicyAdapter}
            }} = ConcurrencyPolicy.resolve(resolved)
  end
end
