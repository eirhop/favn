defmodule Favn.Dev.Reset do
  @moduledoc """
  Removes only the current project's Compose resources and generated state.

  Cleanup is destructive and requires the explicit `yes: true` confirmation.
  The control-plane image is never removed because it is shared and installed
  independently from customer runner images.
  """

  alias Favn.Dev.{ComposeProject, Docker, Lock, Paths, RunnerImage, State}

  @type resource_plan :: %{
          compose_project: String.t(),
          postgres_volume: String.t(),
          local_state: Path.t(),
          runner_images: [String.t()]
        }

  @doc "Returns the exact project-scoped resources that reset would remove."
  @spec plan(keyword()) :: resource_plan()
  def plan(opts \\ []) when is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    project_name = ComposeProject.project_name(root_dir)

    %{
      compose_project: project_name,
      postgres_volume: project_name <> "-postgres-data",
      local_state: Paths.favn_dir(root_dir),
      runner_images: runner_images(root_dir, project_name)
    }
  end

  @doc "Removes the confirmed project-scoped Compose application and `.favn` state."
  @spec run(keyword()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    if Keyword.get(opts, :yes, false) do
      Lock.with_lock(opts, fn -> confirmed_reset(plan(opts), opts) end)
    else
      {:error, {:confirmation_required, plan(opts)}}
    end
  end

  defp confirmed_reset(resources, opts) do
    with :ok <- remove_compose_resources(opts),
         :ok <- Docker.remove_images(resources.runner_images, opts),
         :ok <- remove_favn_dir(resources.local_state) do
      :ok
    end
  end

  defp remove_compose_resources(opts) do
    case State.read_install(opts) do
      {:ok, %{"compose" => %{} = project}} ->
        with {:ok, canonical_project} <- canonical_compose_project(project, opts) do
          case Docker.compose(
                 canonical_project,
                 ["down", "--volumes", "--remove-orphans", "--timeout", "180"],
                 Keyword.put_new(opts, :compose_command_timeout_ms, 240_000)
               ) do
            {_output, 0} -> :ok
            {output, status} -> {:error, {:compose_reset_failed, status, bounded(output)}}
          end
        end

      {:error, :not_found} ->
        :ok

      _invalid ->
        {:error, :install_stale}
    end
  end

  defp canonical_compose_project(project, opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    project_name = ComposeProject.project_name(root_dir)
    compose_path = Paths.compose_path(root_dir)
    env_path = Paths.compose_env_path(root_dir)

    expected = %{
      "project_name" => project_name,
      "network_name" => project_name <> "-network",
      "postgres_volume_name" => project_name <> "-postgres-data",
      "compose_path" => compose_path,
      "env_path" => env_path,
      "runner_env_path" => Paths.compose_runner_env_path(root_dir),
      "postgres_init_path" => Paths.compose_postgres_init_path(root_dir)
    }

    with 1 <- project["schema_version"],
         true <- canonical_fields?(project, expected),
         :ok <- regular_file(compose_path),
         :ok <- regular_file(env_path),
         :ok <- regular_file(expected["runner_env_path"]),
         :ok <- regular_file(expected["postgres_init_path"]),
         {:ok, compose} <- File.read(compose_path),
         true <- project["compose_sha256"] == sha256(compose) do
      {:ok, Map.take(expected, ["project_name", "compose_path", "env_path"])}
    else
      _invalid -> {:error, :install_stale}
    end
  end

  defp canonical_fields?(project, expected) do
    Enum.all?(expected, fn {key, expected_value} ->
      case Map.get(project, key) do
        value
        when key in ["compose_path", "env_path", "runner_env_path", "postgres_init_path"] and
               is_binary(value) ->
          Path.expand(value) == expected_value

        value ->
          value == expected_value
      end
    end)
  end

  defp regular_file(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular}} -> :ok
      _missing_or_unsafe -> {:error, :install_stale}
    end
  end

  defp sha256(contents), do: :crypto.hash(:sha256, contents) |> Base.encode16(case: :lower)

  defp runner_images(root_dir, project_name) do
    root_dir
    |> Paths.dist_target_dir("runner")
    |> Path.join("*/runner-release.json")
    |> Path.wildcard()
    |> Enum.flat_map(fn descriptor_path ->
      with {:ok, bytes} <- File.read(descriptor_path),
           {:ok, descriptor} <- Favn.RunnerRelease.decode(bytes) do
        [RunnerImage.image_reference(project_name, descriptor.runner_release_id)]
      else
        _invalid -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp remove_favn_dir(path) do
    case File.rm_rf(path) do
      {:ok, _entries} -> :ok
      {:error, reason, failed_path} -> {:error, {:reset_failed, reason, failed_path}}
    end
  end

  defp bounded(output) when is_binary(output),
    do: output |> String.trim() |> String.slice(-8_192, 8_192)

  defp bounded(output), do: inspect(output, limit: 20, printable_limit: 1_024)
end
