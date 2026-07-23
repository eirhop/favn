defmodule Favn.Dev.Maintainer.Source do
  @moduledoc """
  Validated local Favn checkout selected for maintainer development.

  The checkout must also provide every Favn path dependency loaded by the
  consuming Mix project. This prevents a local control plane from being paired
  accidentally with official or unrelated compiler and runner code.
  """

  alias Favn.Dev.Build.SourceInputSet
  alias Favn.Dev.Paths

  @checkout_env "FAVN_CHECKOUT"
  @revision ~r/\A[0-9a-f]{40,64}\z/
  @required_dependencies %{
    favn: "apps/favn",
    favn_authoring: "apps/favn_authoring",
    favn_core: "apps/favn_core",
    favn_local: "apps/favn_local",
    favn_runner: "apps/favn_runner",
    favn_sql_runtime: "apps/favn_sql_runtime"
  }
  @optional_dependencies %{
    favn_azure: "apps/favn_azure",
    favn_duckdb: "apps/favn_duckdb",
    favn_duckdb_adbc: "apps/favn_duckdb_adbc",
    favn_orchestrator: "apps/favn_orchestrator",
    favn_storage_postgres: "apps/favn_storage_postgres",
    favn_test_support: "apps/favn_test_support",
    favn_view: "apps/favn_view"
  }

  @control_plane_applications [:favn_core, :favn_orchestrator, :favn_storage_postgres, :favn_view]

  @enforce_keys [:checkout, :revision, :dirty, :fingerprint]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          checkout: Path.t(),
          revision: String.t(),
          dirty: boolean(),
          fingerprint: String.t()
        }

  @doc "Resolves and validates the checkout selected through `FAVN_CHECKOUT`."
  @spec resolve(keyword()) :: {:ok, t()} | {:error, term()}
  def resolve(opts \\ []) when is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    dependency_paths = dependency_paths(opts)

    with {:ok, value} <- checkout_value(opts),
         checkout <- Path.expand(value, root_dir),
         :ok <- validate_dependencies(checkout, dependency_paths),
         application_dirs <- selected_application_dirs(checkout, dependency_paths),
         {:ok, identity} <- identity(checkout, application_dirs, opts) do
      {:ok,
       %__MODULE__{
         checkout: checkout,
         revision: identity.revision,
         dirty: identity.dirty,
         fingerprint: identity.fingerprint
       }}
    end
  end

  @doc false
  @spec fingerprint(Path.t()) :: {:ok, String.t()} | {:error, term()}
  def fingerprint(checkout) when is_binary(checkout) do
    application_dirs = selected_application_dirs(checkout, %{})

    with {:ok, input_set} <- SourceInputSet.maintainer_checkout(checkout, application_dirs) do
      {:ok, SourceInputSet.fingerprint(input_set)}
    else
      _invalid -> {:error, {:maintainer_checkout_fingerprint_failed, checkout}}
    end
  rescue
    _error -> {:error, {:maintainer_checkout_fingerprint_failed, checkout}}
  end

  @doc "Returns the environment variable used by consumer `mix.exs` files."
  @spec environment_variable() :: String.t()
  def environment_variable, do: @checkout_env

  @doc false
  @spec identity(Path.t(), [Path.t()], keyword()) :: {:ok, map()} | {:error, term()}
  def identity(checkout, application_dirs, opts \\ [])
      when is_binary(checkout) and is_list(application_dirs) and is_list(opts) do
    application_dirs =
      (application_dirs ++ default_application_dirs(checkout))
      |> Enum.uniq()
      |> Enum.sort()

    runner =
      if Mix.env() == :test,
        do: Keyword.get(opts, :maintainer_command_runner, &System.cmd/3),
        else: &System.cmd/3

    with {:ok, input_set} <-
           SourceInputSet.maintainer_checkout(checkout, application_dirs),
         {:ok, revision} <- git_revision(checkout, runner),
         {:ok, dirty} <- SourceInputSet.git_dirty?(input_set, runner) do
      {:ok,
       %{
         revision: revision,
         dirty: dirty,
         fingerprint: SourceInputSet.fingerprint(input_set),
         input_set: input_set
       }}
    else
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, {:maintainer_checkout_git_unavailable, Exception.message(error)}}
  end

  defp checkout_value(opts) do
    value =
      if Mix.env() == :test,
        do: Keyword.get(opts, :maintainer_checkout, System.get_env(@checkout_env)),
        else: System.get_env(@checkout_env)

    case value do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, :maintainer_checkout_required}
          trimmed -> {:ok, trimmed}
        end

      _missing ->
        {:error, :maintainer_checkout_required}
    end
  end

  defp dependency_paths(opts) do
    if Mix.env() == :test and Keyword.has_key?(opts, :maintainer_dependency_paths),
      do: Keyword.fetch!(opts, :maintainer_dependency_paths),
      else: Mix.Project.deps_paths()
  end

  defp validate_dependencies(checkout, dependency_paths) when is_map(dependency_paths) do
    with :ok <- validate_dependency_set(checkout, dependency_paths, @required_dependencies) do
      optional = Map.take(@optional_dependencies, Map.keys(dependency_paths))
      validate_dependency_set(checkout, dependency_paths, optional)
    end
  end

  defp validate_dependencies(_checkout, _invalid),
    do: {:error, :invalid_maintainer_dependency_paths}

  defp expanded_path(path) when is_binary(path), do: Path.expand(path)
  defp expanded_path(_missing), do: nil

  defp validate_dependency_set(checkout, dependency_paths, dependencies) do
    Enum.reduce_while(dependencies, :ok, fn {app, relative}, :ok ->
      expected = checkout |> Path.join(relative) |> Path.expand()
      actual = dependency_paths |> Map.get(app) |> expanded_path()

      if actual == expected,
        do: {:cont, :ok},
        else: {:halt, {:error, {:maintainer_dependency_mismatch, app, expected, actual}}}
    end)
  end

  defp selected_application_dirs(checkout, dependency_paths) do
    dependency_dirs =
      dependency_paths
      |> Map.keys()
      |> Enum.filter(&Map.has_key?(all_dependencies(), &1))
      |> Enum.map(&Map.fetch!(all_dependencies(), &1))

    (dependency_dirs ++ default_application_dirs(checkout))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp default_application_dirs(checkout) do
    (@required_dependencies |> Map.keys() |> Kernel.++(@control_plane_applications))
    |> Enum.uniq()
    |> Enum.map(&Map.fetch!(all_dependencies(), &1))
    |> Enum.filter(&File.regular?(Path.join([checkout, &1, "mix.exs"])))
  end

  defp all_dependencies, do: Map.merge(@required_dependencies, @optional_dependencies)

  defp git_revision(checkout, runner) do
    with {revision, 0} <-
           runner.("git", ["-C", checkout, "rev-parse", "--verify", "HEAD^{commit}"],
             stderr_to_stdout: true
           ),
         revision <- String.trim(revision),
         true <- Regex.match?(@revision, revision) do
      {:ok, revision}
    else
      _invalid -> {:error, {:maintainer_checkout_git_unavailable, checkout}}
    end
  end
end
