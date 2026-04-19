defmodule Favn.Dev.State do
  @moduledoc """
  Reads and writes project-local `.favn/` state files.
  """

  alias Favn.Dev.Paths

  @type root_opt :: [root_dir: Path.t()]
  @type read_error :: :not_found | term()

  @doc """
  Ensures `.favn/` folders needed by the current tooling slice.
  """
  @spec ensure_layout(root_opt()) :: :ok | {:error, term()}
  def ensure_layout(opts \\ []) when is_list(opts) do
    root_dir = Paths.root_dir(opts)

    [
      Paths.favn_dir(root_dir),
      Paths.logs_dir(root_dir),
      Paths.data_dir(root_dir),
      Paths.manifests_dir(root_dir),
      Paths.history_dir(root_dir)
    ]
    |> Enum.reduce_while(:ok, fn dir, :ok ->
      case File.mkdir_p(dir) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:mkdir_failed, dir, reason}}}
      end
    end)
  end

  @spec read_runtime(root_opt()) :: {:ok, map()} | {:error, read_error()}
  def read_runtime(opts \\ []) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.runtime_path()
    |> read_json()
  end

  @spec write_runtime(map(), root_opt()) :: :ok | {:error, term()}
  def write_runtime(runtime, opts \\ []) when is_map(runtime) and is_list(opts) do
    with :ok <- ensure_layout(opts) do
      opts
      |> Paths.root_dir()
      |> Paths.runtime_path()
      |> write_json(runtime)
    end
  end

  @spec clear_runtime(root_opt()) :: :ok | {:error, term()}
  def clear_runtime(opts \\ []) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.runtime_path()
    |> delete_if_exists()
  end

  @spec read_manifest_latest(root_opt()) :: {:ok, map()} | {:error, read_error()}
  def read_manifest_latest(opts \\ []) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.latest_manifest_path()
    |> read_json()
  end

  @spec write_manifest_latest(map(), root_opt()) :: :ok | {:error, term()}
  def write_manifest_latest(manifest, opts \\ []) when is_map(manifest) and is_list(opts) do
    with :ok <- ensure_layout(opts) do
      opts
      |> Paths.root_dir()
      |> Paths.latest_manifest_path()
      |> write_json(manifest)
    end
  end

  @spec read_last_failure(root_opt()) :: {:ok, map()} | {:error, read_error()}
  def read_last_failure(opts \\ []) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.last_failure_path()
    |> read_json()
  end

  @spec write_last_failure(map(), root_opt()) :: :ok | {:error, term()}
  def write_last_failure(failure, opts \\ []) when is_map(failure) and is_list(opts) do
    with :ok <- ensure_layout(opts) do
      opts
      |> Paths.root_dir()
      |> Paths.last_failure_path()
      |> write_json(failure)
    end
  end

  @spec read_secrets(root_opt()) :: {:ok, map()} | {:error, read_error()}
  def read_secrets(opts \\ []) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.secrets_path()
    |> read_json()
  end

  @spec write_secrets(map(), root_opt()) :: :ok | {:error, term()}
  def write_secrets(secrets, opts \\ []) when is_map(secrets) and is_list(opts) do
    with :ok <- ensure_layout(opts) do
      opts
      |> Paths.root_dir()
      |> Paths.secrets_path()
      |> write_json(secrets)
    end
  end

  @spec read_json(Path.t()) :: {:ok, map()} | {:error, read_error()}
  defp read_json(path) do
    case File.read(path) do
      {:ok, contents} ->
        case JSON.decode(contents) do
          {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
          {:ok, _other} -> {:error, :invalid_json_shape}
          {:error, reason} -> {:error, {:invalid_json, reason}}
        end

      {:error, :enoent} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, {:read_failed, path, reason}}
    end
  end

  @spec write_json(Path.t(), map()) :: :ok | {:error, term()}
  defp write_json(path, map) when is_map(map) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         encoded <- JSON.encode_to_iodata!(map),
         :ok <- File.write(path, [encoded, "\n"]) do
      :ok
    else
      {:error, reason} -> {:error, {:write_failed, path, reason}}
    end
  end

  @spec delete_if_exists(Path.t()) :: :ok | {:error, term()}
  defp delete_if_exists(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:delete_failed, path, reason}}
    end
  end
end
