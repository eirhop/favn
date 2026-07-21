defmodule Favn.RunnerRelease.RuntimeRoots do
  @moduledoc """
  Explicit, Mix-free roots used to select a customer runner's runtime closure.

  Authoring and build tooling classify roots by why they are needed. Core only
  normalizes the values and exposes their union; it does not inspect projects,
  read application configuration, or resolve OTP applications.
  """

  alias Favn.RunnerRelease.Validation

  defstruct asset_modules: [],
            runtime_input_resolver_modules: [],
            plugin_modules: [],
            supervised_child_modules: [],
            extra_modules: [],
            extra_applications: []

  @type t :: %__MODULE__{
          asset_modules: [String.t()],
          runtime_input_resolver_modules: [String.t()],
          plugin_modules: [String.t()],
          supervised_child_modules: [String.t()],
          extra_modules: [String.t()],
          extra_applications: [String.t()]
        }

  @type error :: {:invalid_runner_release_field, atom(), atom()}

  @module_fields [
    :asset_modules,
    :runtime_input_resolver_modules,
    :plugin_modules,
    :supervised_child_modules,
    :extra_modules
  ]

  @doc "Builds a canonical root selection from explicit categories."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, error()}
  def new(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      attrs |> Map.new() |> new()
    else
      {:error, {:invalid_runner_release_field, :runtime_roots, :expected_map}}
    end
  end

  def new(attrs) when is_map(attrs) do
    with {:ok, module_values} <- normalize_module_fields(attrs),
         {:ok, extra_applications} <- normalize_applications(attrs) do
      {:ok,
       struct!(
         __MODULE__,
         module_values ++ [extra_applications: extra_applications]
       )}
    end
  end

  def new(_attrs),
    do: {:error, {:invalid_runner_release_field, :runtime_roots, :expected_map}}

  @doc "Returns every module root as a sorted unique module-name string."
  @spec module_roots(t()) :: [String.t()]
  def module_roots(%__MODULE__{} = roots) do
    @module_fields
    |> Enum.flat_map(&Map.fetch!(roots, &1))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_module_fields(attrs) do
    Enum.reduce_while(@module_fields, {:ok, []}, fn field, {:ok, acc} ->
      case normalize_list(Validation.fetch_optional(attrs, field, []), field, &module_name/2) do
        {:ok, values} -> {:cont, {:ok, [{field, values} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_applications(attrs) do
    normalize_list(
      Validation.fetch_optional(attrs, :extra_applications, []),
      :extra_applications,
      &application_name/2
    )
  end

  defp normalize_list(values, field, normalizer) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalizer.(value, field) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, normalized |> Enum.uniq() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_list(_values, field, _normalizer),
    do: {:error, {:invalid_runner_release_field, field, :expected_list}}

  defp module_name(value, field), do: Validation.module_name(value, field)
  defp application_name(value, field), do: Validation.identifier(value, field, 128)
end
