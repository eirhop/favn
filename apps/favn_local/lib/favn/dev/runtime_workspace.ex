defmodule Favn.Dev.RuntimeWorkspace do
  @moduledoc false

  alias Favn.Dev.Paths
  alias Favn.Dev.RuntimeSource
  alias Favn.Dev.RuntimeTreePolicy
  alias Favn.Dev.State

  @schema_version 1

  @spec materialize(RuntimeSource.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def materialize(%{root: source_root, kind: kind}, opts) when is_list(opts) do
    root_dir = Paths.root_dir(opts)
    runtime_root = Paths.install_runtime_root_dir(root_dir)

    with :ok <- State.ensure_layout(opts),
         true <- RuntimeSource.valid_runtime_root?(source_root),
         {:ok, _} <- File.rm_rf(runtime_root),
         :ok <- File.mkdir_p(runtime_root),
         :ok <- copy_required_entries(source_root, runtime_root),
         :ok <- write_runtime_metadata(kind, source_root, runtime_root, opts),
         {:ok, runtime} <- read(opts) do
      {:ok, runtime}
    else
      false -> {:error, {:invalid_runtime_source_root, source_root}}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:runtime_materialize_failed, other}}
    end
  end

  @spec read(keyword()) :: {:ok, map()} | {:error, term()}
  def read(opts) when is_list(opts), do: State.read_install_runtime(opts)

  @spec runtime_root(keyword()) :: Path.t()
  def runtime_root(opts) when is_list(opts) do
    opts
    |> Paths.root_dir()
    |> Paths.install_runtime_root_dir()
  end

  defp copy_required_entries(source_root, runtime_root) do
    case RuntimeTreePolicy.reduce_required_entries(source_root, :ok, fn source, relative, :ok ->
      destination = Path.join(runtime_root, relative)

      case copy_entry(source, destination) do
        :ok -> {:ok, :ok}
        {:error, reason} -> {:error, reason}
      end
    end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp copy_entry(source, destination) do
    case File.stat(source) do
      {:ok, %{type: :directory}} ->
        with :ok <- File.mkdir_p(Path.dirname(destination)),
             :ok <- copy_directory(source, destination) do
          :ok
        else
          {:error, reason} -> {:error, {:copy_failed, source, destination, reason}}
        end

      {:ok, _} ->
        with :ok <- File.mkdir_p(Path.dirname(destination)),
             :ok <- File.cp(source, destination) do
          :ok
        else
          {:error, reason} -> {:error, {:copy_failed, source, destination, reason}}
        end

      {:error, reason} ->
        {:error, {:copy_failed, source, destination, reason}}
    end
  end

  defp copy_directory(source, destination) do
    with :ok <- File.mkdir_p(destination) do
      case RuntimeTreePolicy.reduce_child_entries(
             source,
             :ok,
             & &1,
             fn child_source, entry, :ok ->
               child_destination = Path.join(destination, entry)

               case copy_entry(child_source, child_destination) do
                 :ok -> {:ok, :ok}
                 {:error, reason} -> {:error, reason}
               end
             end
           ) do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp write_runtime_metadata(kind, source_root, runtime_root, opts) do
    runtime = %{
      "schema_version" => @schema_version,
      "resolved_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source_kind" => Atom.to_string(kind),
      "source_root" => source_root,
      "materialized_root" => runtime_root,
      "orchestrator_root" => runtime_root,
      "runner_root" => runtime_root,
      "web_root" => Path.join(runtime_root, "web/favn_web")
    }

    State.write_install_runtime(runtime, opts)
  end
end
