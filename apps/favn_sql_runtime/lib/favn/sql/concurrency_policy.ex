defmodule Favn.SQL.ConcurrencyPolicy do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Error

  @enforce_keys [:limit, :scope, :applies_to]
  defstruct [:limit, :scope, :applies_to]

  @type limit :: pos_integer() | :unlimited
  @type applies_to :: :all | :writes
  @type t :: %__MODULE__{
          limit: limit(),
          scope: term(),
          applies_to: applies_to()
        }

  @spec resolve(Resolved.t()) :: {:ok, t()} | {:error, Error.t()}
  def resolve(%Resolved{} = resolved) do
    with {:ok, policy} <- default_policy(resolved),
         {:ok, limit} <- configured_limit(resolved) do
      {:ok, %{policy | limit: limit || policy.limit}}
    end
  end

  @spec unlimited(Resolved.t()) :: t()
  def unlimited(%Resolved{} = resolved) do
    %__MODULE__{limit: :unlimited, scope: {:connection, resolved.name}, applies_to: :writes}
  end

  @spec single_writer(Resolved.t()) :: t()
  def single_writer(%Resolved{} = resolved) do
    %__MODULE__{limit: 1, scope: {:connection, resolved.name}, applies_to: :writes}
  end

  defp default_policy(%Resolved{adapter: adapter} = resolved) do
    case Code.ensure_loaded(adapter) do
      {:module, ^adapter} ->
        if function_exported?(adapter, :default_concurrency_policy, 1) do
          case adapter.default_concurrency_policy(resolved) do
            %__MODULE__{} = policy -> {:ok, policy}
            other -> invalid_policy_error(resolved, other)
          end
        else
          {:ok, unlimited(resolved)}
        end

      {:error, reason} ->
        adapter_load_error(resolved, adapter, reason)
    end
  end

  defp adapter_load_error(resolved, adapter, reason) do
    {:error,
     %Error{
       type: :invalid_config,
       message: "SQL adapter could not be loaded for concurrency policy lookup",
       connection: resolved.name,
       operation: :connect,
       details: %{adapter: adapter, reason: reason}
     }}
  end

  defp configured_limit(%Resolved{config: config} = resolved) when is_map(config) do
    case Map.fetch(config, :write_concurrency) do
      :error -> {:ok, nil}
      {:ok, value} -> normalize_limit(value, resolved)
    end
  end

  defp normalize_limit(:unlimited, _resolved), do: {:ok, :unlimited}
  defp normalize_limit(:single, _resolved), do: {:ok, 1}
  defp normalize_limit(value, _resolved) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_limit(value, resolved) do
    {:error,
     %Error{
       type: :invalid_config,
       message: "connection #{inspect(resolved.name)} has invalid :write_concurrency",
       connection: resolved.name,
       operation: :connect,
       details: %{write_concurrency: value}
     }}
  end

  defp invalid_policy_error(resolved, value) do
    {:error,
     %Error{
       type: :invalid_config,
       message: "SQL adapter returned invalid concurrency policy",
       connection: resolved.name,
       operation: :connect,
       details: %{policy: inspect(value)}
     }}
  end
end
