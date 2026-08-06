defmodule FavnStoragePostgres.DevelopmentUpgrade do
  @moduledoc false

  alias FavnStoragePostgres.DevelopmentConnection
  alias FavnStoragePostgres.Release

  @type stage :: :configuration | :migrate | :grant_runtime | :verify_schema
  @type completed_stage :: :migrate | :grant_runtime | :verify_schema
  @type success :: %{completed: [completed_stage()]}
  @type failure :: %{
          required(:stage) => stage(),
          required(:completed) => [completed_stage()],
          required(:code) => atom(),
          required(:outcome) => :not_started | :unknown,
          optional(:variable) => String.t()
        }

  @spec run(keyword()) :: {:ok, success()} | {:error, failure()}
  def run(opts \\ []) when is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())
    release = Keyword.get(opts, :release, Release)
    progress_fun = Keyword.get(opts, :progress_fun, fn _stage -> :ok end)

    with {:ok, runtime_env} <- DevelopmentConnection.env_for(:runtime, env),
         {:ok, migrator_env} <- DevelopmentConnection.env_for(:migrator, env),
         :ok <- require_separate_urls(runtime_env, migrator_env) do
      execute(
        [
          {:migrate, :migrate, migrator_env},
          {:grant_runtime, :grant_runtime, migrator_env},
          {:verify_schema, :verify_runtime_schema, runtime_env}
        ],
        release,
        progress_fun,
        []
      )
    else
      {:error, {:missing_env, variable}} ->
        {:error,
         %{
           stage: :configuration,
           completed: [],
           code: :missing_environment,
           outcome: :not_started,
           variable: variable
         }}

      {:error, code} when is_atom(code) ->
        {:error, %{stage: :configuration, completed: [], code: code, outcome: :not_started}}
    end
  end

  defp execute([], _release, _progress_fun, completed), do: {:ok, %{completed: completed}}

  defp execute([{stage, function, env} | remaining], release, progress_fun, completed) do
    case apply(release, function, [env]) do
      {:ok, %{operation: ^stage, status: :ok}} ->
        progress_fun.(stage)
        execute(remaining, release, progress_fun, completed ++ [stage])

      {:error, %{operation: ^stage, status: :error, code: code}} when is_atom(code) ->
        {:error, %{stage: stage, completed: completed, code: code, outcome: :unknown}}

      _invalid ->
        {:error, %{stage: stage, completed: completed, code: :invalid_result, outcome: :unknown}}
    end
  end

  defp require_separate_urls(runtime_env, migrator_env) do
    if runtime_env["FAVN_DATABASE_URL"] == migrator_env["FAVN_DATABASE_URL"] do
      {:error, :database_roles_not_separated}
    else
      :ok
    end
  end
end
