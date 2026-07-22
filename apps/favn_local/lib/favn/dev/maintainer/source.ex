defmodule Favn.Dev.Maintainer.Source do
  @moduledoc """
  Validated local Favn checkout selected for maintainer development.

  The checkout must also provide every Favn path dependency loaded by the
  consuming Mix project. This prevents a local control plane from being paired
  accidentally with official or unrelated compiler and runner code.
  """

  alias Favn.Dev.Paths

  @checkout_env "FAVN_CHECKOUT"
  @revision ~r/\A[0-9a-f]{40,64}\z/
  @required_files [
    "mix.exs",
    "apps/favn/mix.exs",
    "apps/favn_local/mix.exs",
    "scripts/control_plane_build_id.exs"
  ]
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

  @ignored_root_directories [".favn", ".git", "_build", "deps", "docs", "test"]
  @ignored_app_directories [".favn", ".git", "_build", "deps", "test"]

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

    with {:ok, value} <- checkout_value(opts),
         checkout <- Path.expand(value, root_dir),
         :ok <- validate_checkout(checkout),
         :ok <- validate_dependencies(checkout, dependency_paths(opts)),
         {:ok, revision, dirty} <- git_identity(checkout, opts),
         {:ok, fingerprint} <- fingerprint(checkout) do
      {:ok,
       %__MODULE__{
         checkout: checkout,
         revision: revision,
         dirty: dirty,
         fingerprint: fingerprint
       }}
    end
  end

  @doc false
  @spec fingerprint(Path.t()) :: {:ok, String.t()} | {:error, term()}
  def fingerprint(checkout) when is_binary(checkout) do
    with {:ok, entries} <- fingerprint_entries(checkout, "") do
      digest =
        entries
        |> Enum.sort()
        |> :erlang.term_to_binary([:deterministic])
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, digest}
    else
      _invalid -> {:error, {:maintainer_checkout_fingerprint_failed, checkout}}
    end
  rescue
    _error -> {:error, {:maintainer_checkout_fingerprint_failed, checkout}}
  end

  @doc "Returns the environment variable used by consumer `mix.exs` files."
  @spec environment_variable() :: String.t()
  def environment_variable, do: @checkout_env

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

  defp validate_checkout(checkout) do
    with {:ok, %{type: :directory}} <- File.lstat(checkout) do
      Enum.reduce_while(@required_files, :ok, fn relative, :ok ->
        path = Path.join(checkout, relative)

        case File.lstat(path) do
          {:ok, %{type: :regular}} -> {:cont, :ok}
          {:ok, %{type: :symlink}} -> {:halt, {:error, {:maintainer_checkout_symlink, path}}}
          _invalid -> {:halt, {:error, {:invalid_maintainer_checkout, checkout}}}
        end
      end)
    else
      {:ok, %{type: :symlink}} -> {:error, {:maintainer_checkout_symlink, checkout}}
      _invalid -> {:error, {:invalid_maintainer_checkout, checkout}}
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

  defp git_identity(checkout, opts) do
    runner =
      if Mix.env() == :test,
        do: Keyword.get(opts, :maintainer_command_runner, &System.cmd/3),
        else: &System.cmd/3

    with {revision, 0} <-
           runner.("git", ["-C", checkout, "rev-parse", "--verify", "HEAD^{commit}"],
             stderr_to_stdout: true
           ),
         revision <- String.trim(revision),
         true <- Regex.match?(@revision, revision),
         {status, 0} <-
           runner.("git", ["-C", checkout, "status", "--porcelain", "--untracked-files=normal"],
             stderr_to_stdout: true
           ) do
      {:ok, revision, String.trim(status) != ""}
    else
      _invalid -> {:error, {:maintainer_checkout_git_unavailable, checkout}}
    end
  rescue
    error -> {:error, {:maintainer_checkout_git_unavailable, Exception.message(error)}}
  end

  defp fingerprint_entries(root, relative) do
    directory = if relative == "", do: root, else: Path.join(root, relative)

    with {:ok, names} <- File.ls(directory) do
      names
      |> Enum.sort()
      |> Enum.reject(&ignored_fingerprint_directory?(relative, &1))
      |> Enum.reduce_while({:ok, []}, fn name, {:ok, entries} ->
        child_relative = if relative == "", do: name, else: Path.join(relative, name)
        child = Path.join(root, child_relative)

        case File.lstat(child) do
          {:ok, %{type: :directory}} ->
            case fingerprint_entries(root, child_relative) do
              {:ok, children} -> {:cont, {:ok, children ++ entries}}
              {:error, _reason} = error -> {:halt, error}
            end

          {:ok, %{type: :regular, mode: mode}} ->
            case File.read(child) do
              {:ok, contents} ->
                executable = if Bitwise.band(mode, 0o111) == 0, do: 0, else: 1
                {:cont, {:ok, [{child_relative, executable, contents} | entries]}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end

          {:ok, %{type: :symlink}} ->
            case File.read_link(child) do
              {:ok, target} -> {:cont, {:ok, [{child_relative, :symlink, target} | entries]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          _invalid ->
            {:halt, {:error, :invalid_checkout_entry}}
        end
      end)
    end
  end

  defp ignored_fingerprint_directory?("", name), do: name in @ignored_root_directories

  defp ignored_fingerprint_directory?(relative, name) do
    case Path.split(relative) do
      ["apps", _app] -> name in @ignored_app_directories
      _other -> false
    end
  end
end
