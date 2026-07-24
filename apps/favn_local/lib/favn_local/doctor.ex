defmodule FavnLocal.Doctor do
  @moduledoc false

  alias FavnLocal.Config
  alias FavnLocal.Preflight
  alias FavnLocal.Publication

  @type check :: %{name: String.t(), status: :ok | :error, message: String.t()}

  @spec run(keyword()) :: {:ok, [check()]} | {:error, [check()]}
  def run(opts \\ []) when is_list(opts) do
    case Config.load(opts) do
      {:ok, config} ->
        :ok = Config.apply(config)

        checks = [
          ok("environment", "required environment variables are valid"),
          postgres_check(config),
          authoring_check(config)
        ]

        result(checks)

      {:error, reason} ->
        {:error, [error("environment", format_reason(reason))]}
    end
  end

  defp postgres_check(config) do
    case Preflight.run(config) do
      :ok -> ok("PostgreSQL", "schema and workspace #{config.workspace_id} are ready")
      {:error, reason} -> error("PostgreSQL", format_reason(reason))
    end
  end

  defp authoring_check(config) do
    case Publication.build(config.runner_release_id) do
      {:ok, publication} ->
        ok("project", "manifest #{publication.version.manifest_version_id} compiles")

      {:error, reason} ->
        error("project", format_reason(reason))
    end
  end

  defp result(checks) do
    if Enum.all?(checks, &(&1.status == :ok)), do: {:ok, checks}, else: {:error, checks}
  end

  defp ok(name, message), do: %{name: name, status: :ok, message: message}
  defp error(name, message), do: %{name: name, status: :error, message: message}

  defp format_reason({:missing_env, name}), do: "missing required environment variable #{name}"

  defp format_reason({:postgres_schema_not_ready, command}),
    do: "schema is not ready; run #{command}"

  defp format_reason({:workspace_not_found, workspace_id, command}),
    do: "workspace #{workspace_id} is not provisioned; run #{command}"

  defp format_reason(reason), do: inspect(reason)
end
