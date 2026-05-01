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
              admission_timeout_ms: :infinity,
              connection: :warehouse
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
