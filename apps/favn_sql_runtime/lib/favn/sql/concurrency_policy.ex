defmodule Favn.SQL.ConcurrencyPolicy do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Error

  @enforce_keys [:limit, :scope, :applies_to]
  defstruct [:limit, :scope, :applies_to, :connection, :target, admission_timeout_ms: :infinity]

  @type limit :: pos_integer() | :unlimited
  @type applies_to :: :all | :writes
  @type admission_timeout_ms :: pos_integer() | :infinity
  @type t :: %__MODULE__{
          limit: limit(),
           scope: term(),
           applies_to: applies_to(),
           connection: atom() | nil,
           target: Favn.SQL.ConcurrencyPolicies.target() | nil,
           admission_timeout_ms: admission_timeout_ms()
         }

  @spec resolve(Resolved.t()) :: {:ok, t() | Favn.SQL.ConcurrencyPolicies.t()} | {:error, Error.t()}
  def resolve(%Resolved{} = resolved) do
    with {:ok, policies} <- adapter_policies(resolved),
         {:ok, limit} <- configured_limit(resolved),
         {:ok, admission_timeout_ms} <- configured_admission_timeout(resolved) do
      {:ok, apply_configured_overrides(policies, resolved, limit, admission_timeout_ms)}
    end
  end

  @spec unlimited(Resolved.t()) :: t()
  def unlimited(%Resolved{} = resolved) do
    %__MODULE__{
      limit: :unlimited,
      scope: {:connection, resolved.name},
      applies_to: :writes,
      connection: resolved.name,
      target: :default
    }
  end

  @spec single_writer(Resolved.t()) :: t()
  def single_writer(%Resolved{} = resolved) do
    %__MODULE__{
      limit: 1,
      scope: {:connection, resolved.name},
      applies_to: :writes,
      connection: resolved.name,
      target: :default
    }
  end

  @spec catalog(Resolved.t(), binary(), limit()) :: t()
  def catalog(%Resolved{} = resolved, catalog, limit) when is_binary(catalog) do
    %__MODULE__{
      limit: limit,
      scope: {resolved.name, catalog},
      applies_to: :writes,
      connection: resolved.name,
      target: {:catalog, catalog}
    }
  end

  defp adapter_policies(%Resolved{adapter: adapter} = resolved) do
    case Code.ensure_loaded(adapter) do
      {:module, ^adapter} ->
        cond do
          function_exported?(adapter, :concurrency_policies, 1) ->
            resolve_adapter_policies(resolved, adapter)

          function_exported?(adapter, :default_concurrency_policy, 1) ->
            case adapter.default_concurrency_policy(resolved) do
              %__MODULE__{} = policy -> {:ok, normalize_policy(policy, resolved, :default)}
              other -> invalid_policy_error(resolved, other)
            end

          true ->
            {:ok, unlimited(resolved)}
        end

      {:error, reason} ->
        adapter_load_error(resolved, adapter, reason)
    end
  end

  defp resolve_adapter_policies(resolved, adapter) do
    case adapter.concurrency_policies(resolved) do
      {:ok, policies} when is_list(policies) ->
        build_policy_container(resolved, policies)

      {:error, %Error{}} = error ->
        error

      other ->
        invalid_policy_error(resolved, other)
    end
  end

  defp build_policy_container(resolved, policies) do
    with {:ok, normalized} <- normalize_policy_list(resolved, policies) do
      default = Enum.find(normalized, &(Map.get(&1, :target) == :default)) || unlimited(resolved)

      catalog = Enum.reject(normalized, &(Map.get(&1, :target) == :default))
      {:ok, Favn.SQL.ConcurrencyPolicies.new(default, catalog)}
    end
  end

  defp normalize_policy_list(resolved, policies) do
    policies
    |> Enum.reduce_while({:ok, []}, fn
      %__MODULE__{} = policy, {:ok, acc} ->
        {:cont, {:ok, [normalize_policy(policy, resolved, policy.target || :default) | acc]}}

      other, _acc ->
        {:halt, invalid_policy_error(resolved, other)}
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp normalize_policy(%__MODULE__{} = policy, %Resolved{} = resolved, target) do
    %{policy | connection: policy.connection || resolved.name, target: target}
  end

  defp apply_configured_overrides(%Favn.SQL.ConcurrencyPolicies{} = policies, resolved, limit, timeout) do
    %Favn.SQL.ConcurrencyPolicies{
      policies
      | default: apply_configured_overrides(policies.default, resolved, limit, timeout),
        catalog: Map.new(policies.catalog, fn {key, policy} -> {key, apply_timeout(policy, resolved, timeout)} end)
    }
  end

  defp apply_configured_overrides(%__MODULE__{} = policy, resolved, limit, timeout) do
    %{
      policy
      | limit: limit || policy.limit,
        connection: policy.connection || resolved.name,
        target: policy.target || :default,
        admission_timeout_ms: timeout || policy.admission_timeout_ms
    }
  end

  defp apply_configured_overrides(nil, _resolved, _limit, _timeout), do: nil

  defp apply_timeout(%__MODULE__{} = policy, resolved, timeout) do
    %{
      policy
      | connection: policy.connection || resolved.name,
        admission_timeout_ms: timeout || policy.admission_timeout_ms
    }
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

  defp configured_admission_timeout(%Resolved{config: config} = resolved) when is_map(config) do
    case Map.fetch(config, :admission_timeout_ms) do
      :error -> {:ok, nil}
      {:ok, value} -> normalize_admission_timeout(value, resolved)
    end
  end

  defp normalize_admission_timeout(:infinity, _resolved), do: {:ok, :infinity}

  defp normalize_admission_timeout(value, _resolved)
       when is_integer(value) and value > 0,
       do: {:ok, value}

  defp normalize_admission_timeout(value, resolved) do
    {:error,
     %Error{
       type: :invalid_config,
       message: "connection #{inspect(resolved.name)} has invalid :admission_timeout_ms",
       connection: resolved.name,
       operation: :connect,
       details: %{admission_timeout_ms: value}
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
