defmodule Favn.Dev.Config do
  @moduledoc """
  Resolves minimal local developer tooling configuration.
  """

  @enforce_keys [
    :workspace_id,
    :orchestrator_port,
    :web_port,
    :scheduler_enabled,
    :compose_file
  ]
  defstruct [
    :workspace_id,
    :orchestrator_port,
    :web_port,
    :scheduler_enabled,
    :compose_file
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          orchestrator_port: pos_integer(),
          web_port: pos_integer(),
          scheduler_enabled: boolean(),
          compose_file: Path.t()
        }

  @typedoc "Keyword overrides used by local tooling tasks."
  @type opts :: keyword()

  @default_orchestrator_port 4101
  @default_web_port 4173
  @default_compose_file "deploy/compose.local.yml"

  @doc """
  Resolves local tooling configuration from app config plus runtime overrides.
  """
  @spec resolve(opts()) :: t()
  def resolve(opts \\ []) when is_list(opts) do
    dev_config = Application.get_env(:favn, :dev, [])
    local_config = Application.get_env(:favn, :local, [])
    merged = dev_config |> Keyword.merge(local_config) |> Keyword.merge(opts)

    orchestrator_port =
      merged
      |> Keyword.get(:orchestrator_port, @default_orchestrator_port)
      |> normalize_int(@default_orchestrator_port)

    web_port =
      merged
      |> Keyword.get(:web_port, @default_web_port)
      |> normalize_int(@default_web_port)

    %__MODULE__{
      workspace_id: normalize_workspace_id(Keyword.get(merged, :workspace_id, "local-dev")),
      orchestrator_port: orchestrator_port,
      web_port: web_port,
      scheduler_enabled: normalize_bool(Keyword.get(merged, :scheduler, false), false),
      compose_file: compose_file_value(opts, local_config)
    }
  end

  @doc "Resolves and validates the selected local Compose file."
  @spec resolve_compose_file(opts()) :: {:ok, Path.t()} | {:error, term()}
  def resolve_compose_file(opts \\ []) when is_list(opts) do
    root_dir = opts |> Favn.Dev.Paths.root_dir() |> Path.expand()
    local_config = Application.get_env(:favn, :local, [])
    value = compose_file_value(opts, local_config)

    with true <- is_binary(value) and String.trim(value) != "",
         path <- Path.expand(value, root_dir),
         :ok <- inside_project(path, root_dir),
         :ok <- regular_non_symlink(path),
         :ok <- non_symlinked_parents(path, root_dir) do
      {:ok, path}
    else
      false -> {:error, {:invalid_compose_file, value}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns the project-relative built-in local Compose default."
  @spec default_compose_file() :: String.t()
  def default_compose_file, do: @default_compose_file

  defp compose_file_value(opts, local_config) do
    case Keyword.fetch(opts, :compose_file) do
      {:ok, value} -> value
      :error -> Keyword.get(local_config, :compose_file, @default_compose_file)
    end
  end

  defp inside_project(path, root_dir) do
    relative = Path.relative_to(path, root_dir)

    if relative != ".." and not String.starts_with?(relative, "../") and relative != path,
      do: :ok,
      else: {:error, {:compose_file_outside_project, path}}
  end

  defp regular_non_symlink(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular}} -> :ok
      {:ok, %{type: :symlink}} -> {:error, {:compose_file_symlink, path}}
      {:ok, _other} -> {:error, {:compose_file_not_regular, path}}
      {:error, :enoent} -> {:error, {:compose_file_missing, path}}
      {:error, reason} -> {:error, {:compose_file_unreadable, path, reason}}
    end
  end

  defp non_symlinked_parents(path, root_dir) do
    path
    |> Path.dirname()
    |> parent_paths(root_dir)
    |> Enum.reduce_while(:ok, fn parent, :ok ->
      case File.lstat(parent) do
        {:ok, %{type: :directory}} -> {:cont, :ok}
        {:ok, %{type: :symlink}} -> {:halt, {:error, {:compose_file_symlink, path}}}
        _invalid -> {:halt, {:error, {:compose_file_unreadable, path, :unsafe_parent}}}
      end
    end)
  end

  defp parent_paths(path, root_dir) do
    relative_parts = path |> Path.relative_to(root_dir) |> Path.split()

    for count <- length(relative_parts)..0//-1 do
      case Enum.take(relative_parts, count) do
        [] -> root_dir
        parts -> Path.join([root_dir | parts])
      end
    end
  end

  defp normalize_int(value, _default) when is_integer(value) and value > 0, do: value

  defp normalize_int(value, default) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp normalize_int(_value, default), do: default

  defp normalize_bool(value, _default) when is_boolean(value), do: value
  defp normalize_bool("true", _default), do: true
  defp normalize_bool("false", _default), do: false
  defp normalize_bool(_value, default), do: default

  defp normalize_workspace_id(value) when is_binary(value) do
    case String.trim(value) do
      id when id != "" and byte_size(id) <= 255 -> id
      _invalid -> raise ArgumentError, "local workspace_id must contain 1..255 bytes"
    end
  end

  defp normalize_workspace_id(_value),
    do: raise(ArgumentError, "local workspace_id must be a string")
end
