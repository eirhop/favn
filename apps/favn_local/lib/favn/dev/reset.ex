defmodule Favn.Dev.Reset do
  @moduledoc """
  Removes generated local Favn state without deleting consumer-owned resources.

  Reset never runs Compose `down`, removes volumes or networks, stops consumer
  services, deletes the selected Compose file, or deletes `.data`.
  """

  alias Favn.Dev.{
    ComposeDeployment,
    ComposeProject,
    Config,
    Docker,
    Lock,
    Paths,
    State
  }

  @type resource_plan :: %{
          compose_project: String.t(),
          generated_state: Path.t(),
          preserved_data: Path.t(),
          preserved_compose_file: Path.t() | nil
        }

  @doc "Returns the exact generated resources reset would remove and preserve."
  @spec plan(keyword()) :: resource_plan()
  def plan(opts \\ []) when is_list(opts) do
    root_dir = opts |> Paths.root_dir() |> Path.expand()
    project_name = ComposeProject.project_name(root_dir)

    %{
      compose_project: project_name,
      generated_state: Paths.favn_dir(root_dir),
      preserved_data: Paths.local_data_dir(root_dir),
      preserved_compose_file: selected_compose_file(opts)
    }
  end

  @doc "Removes confirmed generated state after proving known Favn roles are stopped."
  @spec run(keyword()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    if Keyword.get(opts, :yes, false) do
      favn_dir = opts |> Paths.root_dir() |> Path.expand() |> Paths.favn_dir()

      with :ok <- ensure_safe_state_root(favn_dir) do
        Lock.with_lock(opts, fn -> confirmed_reset(plan(opts), opts) end)
      end
    else
      {:error, {:confirmation_required, plan(opts)}}
    end
  end

  defp confirmed_reset(resources, opts) do
    with :ok <- ensure_safe_state_root(resources.generated_state),
         :ok <- ensure_known_roles_stopped(opts),
         :ok <- remove_generated_state(resources) do
      :ok
    end
  end

  defp ensure_known_roles_stopped(opts) do
    case State.read_runtime(opts) do
      {:error, :not_found} ->
        ensure_unrecorded_roles_stopped(opts)

      {:ok, runtime} ->
        with {:ok, deployment} <- ComposeDeployment.from_runtime(runtime, opts),
             {output, 0} <-
               Docker.compose(deployment, ["ps", "--status", "running", "--services"], opts) do
          running = output |> String.split("\n", trim: true) |> MapSet.new()
          favn = deployment.services |> Map.values() |> MapSet.new()
          active = MapSet.intersection(running, favn) |> MapSet.to_list() |> Enum.sort()

          if active == [], do: :ok, else: {:error, {:reset_roles_running, active}}
        else
          {_output, status} when is_integer(status) ->
            {:error, {:reset_role_status_unavailable, status}}

          {:error, _reason} = error ->
            error
        end

      {:error, reason} ->
        {:error, {:local_runtime_state_unavailable, reason}}
    end
  end

  defp ensure_unrecorded_roles_stopped(opts) do
    project_name = opts |> Paths.root_dir() |> Path.expand() |> ComposeProject.project_name()

    case Docker.project_role_containers(project_name, opts) do
      {:ok, containers} ->
        active =
          containers
          |> Enum.filter(& &1.running?)
          |> Enum.map(& &1.name)
          |> Enum.sort()

        if active == [], do: :ok, else: {:error, {:reset_roles_running, active}}

      {:error, reason} ->
        {:error, {:reset_role_status_unavailable, reason}}
    end
  end

  defp selected_compose_file(opts) do
    case State.read_runtime(opts) do
      {:ok, %{"compose_file" => path}} when is_binary(path) ->
        path

      _unavailable ->
        case State.read_compose_selection(opts) do
          {:ok, %{"compose_file" => path}} when is_binary(path) ->
            path

          _unavailable ->
            case Config.resolve_compose_file(opts) do
              {:ok, path} -> path
              {:error, _reason} -> nil
            end
        end
    end
  end

  defp ensure_safe_state_root(favn_dir) do
    case File.lstat(favn_dir) do
      {:ok, %{type: :directory}} -> :ok
      {:error, :enoent} -> :ok
      {:ok, _unsafe} -> {:error, {:unsafe_generated_state, favn_dir}}
      {:error, reason} -> {:error, {:reset_failed, reason, favn_dir}}
    end
  end

  defp remove_generated_state(resources) do
    favn_dir = resources.generated_state
    root_dir = Path.dirname(favn_dir)
    protected = protected_consumer_paths(resources, favn_dir)

    generated_files = [
      Paths.runtime_path(root_dir),
      Paths.secrets_path(root_dir),
      Paths.maintenance_path(root_dir)
    ]

    generated_directories = [
      Paths.install_dir(root_dir),
      Paths.compose_dir(root_dir),
      Paths.build_dir(root_dir),
      Paths.dist_dir(root_dir),
      Paths.manifests_dir(root_dir),
      Paths.history_dir(root_dir),
      Paths.logs_dir(root_dir)
    ]

    (generated_files ++ generated_directories)
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case remove_generated_path(path, protected) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp protected_consumer_paths(resources, favn_dir) do
    case resources.preserved_compose_file do
      path when is_binary(path) ->
        compose = Path.expand(path)
        companion = Path.rootname(compose) <> ".env.example"

        [compose, companion]
        |> Enum.filter(&inside?(&1, favn_dir))
        |> MapSet.new()

      _unavailable ->
        MapSet.new()
    end
  end

  defp remove_generated_path(path, protected) do
    cond do
      MapSet.member?(protected, path) ->
        :ok

      true ->
        case File.lstat(path) do
          {:error, :enoent} ->
            :ok

          {:ok, %{type: :regular}} ->
            remove_file(path)

          {:ok, %{type: :directory}} ->
            remove_generated_directory(path, protected)

          {:ok, _unsafe} ->
            {:error, {:unsafe_generated_state, path}}

          {:error, reason} ->
            {:error, {:reset_failed, reason, path}}
        end
    end
  end

  defp remove_generated_directory(path, protected) do
    with {:ok, entries} <- File.ls(path),
         :ok <- remove_generated_entries(path, entries, protected) do
      case File.rmdir(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} when reason in [:eexist, :enotempty] -> :ok
        {:error, reason} -> {:error, {:reset_failed, reason, path}}
      end
    else
      {:error, {:reset_failed, _reason, _path}} = error -> error
      {:error, {:unsafe_generated_state, _path}} = error -> error
      {:error, reason} -> {:error, {:reset_failed, reason, path}}
    end
  end

  defp remove_generated_entries(path, entries, protected) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      child = Path.join(path, entry)

      case remove_generated_path(child, protected) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp inside?(path, parent) do
    relative = Path.relative_to(path, parent)
    relative != path and relative != ".." and not String.starts_with?(relative, "../")
  end

  defp remove_file(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:reset_failed, reason, path}}
    end
  end
end
