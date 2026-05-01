defmodule FavnRunner.ProductionRuntimeConfig do
  @moduledoc """
  Production runtime configuration for the runner process.

  This module owns the runner-side production environment contract. The first
  supported production topology is a local, single-node runner only; distributed
  or remote runner modes are intentionally rejected here.
  """

  @type mode :: :local
  @type topology :: :single_node
  @type config :: %{
          mode: mode(),
          topology: topology()
        }

  @doc """
  Validates and applies production env config.
  """
  @spec apply_from_env(map()) :: :ok | {:error, map()}
  def apply_from_env(env \\ System.get_env()) when is_map(env) do
    with {:ok, config} <- validate(env) do
      Application.put_env(:favn_runner, :production_runtime_config, config)
      Application.put_env(:favn_runner, :production_runtime_diagnostics, diagnostics(config))

      :ok
    end
  end

  @doc """
  Validates production runner env values without mutating application env.

  `FAVN_RUNNER_MODE` defaults to `local` when unset. Explicit values must be
  exactly `local`; `remote`, `distributed`, and other malformed values are not
  supported by the first single-node runner setup.
  """
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env \\ System.get_env()) when is_map(env) do
    case runner_mode(env) do
      {:ok, mode} -> {:ok, %{mode: mode, topology: :single_node}}
      {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
    end
  end

  @doc """
  Returns redacted diagnostics for a validated runner config.
  """
  @spec diagnostics(config()) :: map()
  def diagnostics(config) when is_map(config) do
    %{
      status: :ok,
      runner: %{
        mode: Map.fetch!(config, :mode),
        topology: Map.fetch!(config, :topology)
      }
    }
  end

  defp runner_mode(env) do
    case fetch(env, "FAVN_RUNNER_MODE") do
      {:ok, "local"} -> {:ok, :local}
      {:ok, other} -> {:error, {:invalid_env, "FAVN_RUNNER_MODE", other, "local"}}
      :error -> {:ok, :local}
    end
  end

  defp fetch(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: :error, else: {:ok, value}

      _other ->
        :error
    end
  end

  defp redact({:invalid_env, name, _value, expected}), do: {:invalid_env, name, expected}
end
