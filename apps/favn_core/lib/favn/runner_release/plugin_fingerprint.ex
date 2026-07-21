defmodule Favn.RunnerRelease.PluginFingerprint do
  @moduledoc """
  Canonical plugin or adapter identity embedded in a runner release.

  Capabilities are declarative names only. Credentials and resolved runtime
  configuration are never part of this value.
  """

  alias Favn.RunnerRelease.Validation

  @enforce_keys [:plugin, :version, :modules, :capabilities]
  defstruct [:plugin, :version, modules: [], capabilities: []]

  @type t :: %__MODULE__{
          plugin: String.t(),
          version: String.t(),
          modules: [String.t()],
          capabilities: [String.t()]
        }

  @type error ::
          {:missing_runner_release_field, atom()}
          | {:invalid_runner_release_field, atom(), atom()}
          | {:duplicate_runner_release_entry, atom(), String.t()}

  @doc "Builds and validates a plugin fingerprint."
  @spec new(map() | t()) :: {:ok, t()} | {:error, error()}
  def new(value) when is_map(value) do
    with {:ok, plugin} <- required_identifier(value, :plugin, 255),
         {:ok, version} <- required_string(value, :version, 128),
         {:ok, modules} <- required_modules(value),
         {:ok, capabilities} <- required_capabilities(value) do
      {:ok,
       %__MODULE__{
         plugin: plugin,
         version: version,
         modules: modules,
         capabilities: capabilities
       }}
    end
  end

  def new(_value), do: {:error, {:invalid_runner_release_field, :plugins, :expected_map}}

  @doc "Returns the canonical identity payload for this plugin."
  @spec identity_payload(t()) :: map()
  def identity_payload(%__MODULE__{} = fingerprint) do
    %{
      "plugin" => fingerprint.plugin,
      "version" => fingerprint.version,
      "modules" => fingerprint.modules,
      "capabilities" => fingerprint.capabilities
    }
  end

  defp normalized_modules(values) do
    normalize_unique(values, :modules, &Validation.module_name(&1, :modules))
  end

  defp normalized_capabilities(values) do
    normalize_unique(values, :capabilities, &Validation.identifier(&1, :capabilities, 128))
  end

  defp required_modules(value) do
    case Validation.fetch(value, :modules) do
      {:ok, modules} -> normalized_modules(modules)
      {:error, _reason} = error -> error
    end
  end

  defp required_capabilities(value) do
    case Validation.fetch(value, :capabilities) do
      {:ok, capabilities} -> normalized_capabilities(capabilities)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_unique(values, field, normalizer) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalizer.(value) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> reject_duplicates_and_sort(normalized, field)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_unique(_values, field, _normalizer),
    do: {:error, {:invalid_runner_release_field, field, :expected_list}}

  defp reject_duplicates_and_sort(values, field) do
    case duplicate(values) do
      nil -> {:ok, Enum.sort(values)}
      value -> {:error, {:duplicate_runner_release_entry, field, value}}
    end
  end

  defp duplicate(values) do
    values
    |> Enum.frequencies()
    |> Enum.find_value(fn {value, count} -> if count > 1, do: value end)
  end

  defp required_identifier(value, field, max_bytes) do
    case Validation.fetch(value, field) do
      {:ok, field_value} -> Validation.identifier(field_value, field, max_bytes)
      {:error, _reason} = error -> error
    end
  end

  defp required_string(value, field, max_bytes) do
    case Validation.fetch(value, field) do
      {:ok, field_value} -> Validation.string(field_value, field, max_bytes)
      {:error, _reason} = error -> error
    end
  end
end
