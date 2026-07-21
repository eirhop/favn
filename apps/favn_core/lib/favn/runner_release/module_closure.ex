defmodule Favn.RunnerRelease.ModuleClosure do
  @moduledoc """
  Pure transitive module selection for runner release fingerprints.

  Callers provide the complete set of project-local BEAM binaries that may be
  selected. Imports outside that explicit set are represented later by OTP
  application fingerprints and are not pulled into this closure. Explicit
  dynamic modules are treated as roots and must be present.
  """

  alias Favn.RunnerRelease.BeamDigest
  alias Favn.RunnerRelease.ModuleFingerprint
  alias Favn.RunnerRelease.RuntimeRoots
  alias Favn.RunnerRelease.Validation

  @enforce_keys [:root_modules, :modules, :extra_applications]
  defstruct root_modules: [], modules: [], extra_applications: []

  @type t :: %__MODULE__{
          root_modules: [String.t()],
          modules: [ModuleFingerprint.t()],
          extra_applications: [String.t()]
        }

  @type error ::
          {:invalid_module_collection, :expected_map_or_list}
          | {:invalid_module_entry, term()}
          | {:duplicate_available_module, String.t()}
          | {:beam_module_name_mismatch, String.t(), String.t()}
          | {:missing_runtime_root_module, String.t()}
          | {:invalid_runtime_module, String.t(), BeamDigest.error()}
          | BeamDigest.error()
          | {:invalid_runner_release_field, atom(), atom()}

  @doc """
  Selects runtime roots and their project-local transitive imports.

  `available_modules` is a map or keyword-like list of module name to compiled
  BEAM binary. `:extra_modules` adds dynamic roots that static imports cannot
  discover.
  """
  @spec build(RuntimeRoots.t(), map() | list()) :: {:ok, t()} | {:error, error()}
  def build(%RuntimeRoots{} = runtime_roots, available_modules) do
    roots = RuntimeRoots.module_roots(runtime_roots)

    with {:ok, available} <- normalize_available(available_modules),
         {:ok, analyzed} <- analyze_available(available),
         :ok <- require_roots(roots, analyzed),
         {:ok, selected} <- traverse(roots, analyzed) do
      fingerprints =
        selected
        |> Enum.map(fn {_name, metadata} ->
          %ModuleFingerprint{module: metadata.module, digest: metadata.digest}
        end)
        |> Enum.sort_by(& &1.module)

      {:ok,
       %__MODULE__{
         root_modules: roots,
         modules: fingerprints,
         extra_applications: runtime_roots.extra_applications
       }}
    end
  end

  @spec build([module() | String.t()], map() | list()) :: {:ok, t()} | {:error, error()}
  def build(root_modules, available_modules) when is_list(root_modules),
    do: build(root_modules, available_modules, [])

  @spec build([module() | String.t()], map() | list(), keyword()) ::
          {:ok, t()} | {:error, error()}
  def build(root_modules, available_modules, opts)
      when is_list(root_modules) and is_list(opts) do
    with {:ok, roots} <-
           RuntimeRoots.new(%{
             asset_modules: root_modules,
             extra_modules: Keyword.get(opts, :extra_modules, []),
             extra_applications: Keyword.get(opts, :extra_applications, [])
           }) do
      build(roots, available_modules)
    end
  end

  def build(_root_modules, _available_modules, _opts),
    do: {:error, {:invalid_runner_release_field, :runtime_modules, :expected_list}}

  defp normalize_available(values) when is_map(values),
    do: normalize_available(Enum.to_list(values))

  defp normalize_available(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, %{}}, fn
      {declared_name, beam}, {:ok, acc} when is_binary(beam) ->
        with {:ok, name} <- Validation.module_name(declared_name, :runtime_modules) do
          if Map.has_key?(acc, name) do
            {:halt, {:error, {:duplicate_available_module, name}}}
          else
            {:cont, {:ok, Map.put(acc, name, beam)}}
          end
        else
          {:error, _reason} = error -> {:halt, error}
        end

      entry, _acc ->
        {:halt, {:error, {:invalid_module_entry, entry}}}
    end)
  end

  defp normalize_available(_values),
    do: {:error, {:invalid_module_collection, :expected_map_or_list}}

  defp require_roots(roots, available) do
    case Enum.find(roots, &(not Map.has_key?(available, &1))) do
      nil -> :ok
      name -> {:error, {:missing_runtime_root_module, name}}
    end
  end

  defp traverse(roots, available) do
    protocol_implementations =
      available
      |> Enum.flat_map(fn {name, metadata} ->
        case metadata do
          {:ok, %{protocol_implementation: implementation}} when not is_nil(implementation) ->
            [name]

          {:error, _reason, implementation} when not is_nil(implementation) ->
            [name]

          _not_an_implementation ->
            []
        end
      end)
      |> Enum.sort()

    # Protocol dispatch is dynamic and a local implementation may target a
    # dependency protocol that is absent from the project-local module set.
    # Conservatively including every local defimpl prevents an implementation
    # edit from escaping the runner release identity.
    visit(Enum.uniq(roots ++ protocol_implementations), available, %{}, MapSet.new())
  end

  defp visit([], _available, selected, _queued), do: {:ok, selected}

  defp visit([name | rest], available, selected, queued) do
    if Map.has_key?(selected, name) do
      visit(rest, available, selected, queued)
    else
      case Map.fetch!(available, name) do
        {:ok, metadata} ->
          imports =
            metadata.imports
            |> Enum.filter(&Map.has_key?(available, &1))
            |> Enum.reject(&MapSet.member?(queued, &1))
            |> Enum.uniq()
            |> Enum.sort()

          visit(
            rest ++ imports,
            available,
            Map.put(selected, name, metadata),
            Enum.reduce(imports, MapSet.put(queued, name), &MapSet.put(&2, &1))
          )

        {:error, reason, _implementation} ->
          invalid_module_error(name, reason)

        {:error, reason} ->
          invalid_module_error(name, reason)
      end
    end
  end

  defp invalid_module_error(_name, {:beam_module_name_mismatch, _declared, _actual} = reason),
    do: {:error, reason}

  defp invalid_module_error(name, reason),
    do: {:error, {:invalid_runtime_module, name, reason}}

  defp analyze_available(available) do
    analyzed =
      Map.new(available, fn {name, beam} ->
        result = analyze_module(name, beam)

        {name, result}
      end)

    {:ok, analyzed}
  end

  defp analyze_module(name, beam) do
    with {:ok, metadata} <- BeamDigest.metadata(beam),
         :ok <- match_module_name(name, metadata.module) do
      {:ok, metadata}
    else
      {:error, reason} ->
        case BeamDigest.protocol_implementation_metadata(beam) do
          {:ok, implementation} -> {:error, reason, implementation}
          {:error, _attribute_reason} -> {:error, reason}
        end
    end
  end

  defp match_module_name(name, name), do: :ok

  defp match_module_name(declared, actual),
    do: {:error, {:beam_module_name_mismatch, declared, actual}}
end
