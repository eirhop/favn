defmodule FavnLocal.SourceRelease do
  @moduledoc """
  Derives the local runner release identity from compiled source inputs.

  Source development requires a full stop/start for dependency, plugin, native
  library, and configuration changes. Within one running source-development
  session, the compiled BEAM closure is therefore the exact input that decides
  whether a runner replacement is necessary.
  """

  @spec current(keyword()) :: {:ok, String.t()} | {:error, term()}
  def current(opts \\ []) when is_list(opts) do
    build_path = Keyword.get_lazy(opts, :build_path, &Mix.Project.build_path/0)

    files =
      build_path
      |> Path.join("lib/*/ebin/*.beam")
      |> Path.wildcard()
      |> Enum.sort()

    case files do
      [] ->
        {:error, {:compiled_source_unavailable, Path.expand(build_path)}}

      files ->
        with {:ok, digest} <- digest_files(files, build_path) do
          {:ok, "rr_" <> Base.encode16(digest, case: :lower)}
        end
    end
  end

  defp digest_files(files, build_path) do
    files
    |> Enum.reduce_while({:ok, :crypto.hash_init(:sha256)}, fn path, {:ok, digest} ->
      case File.read(path) do
        {:ok, bytes} ->
          relative = Path.relative_to(path, build_path)

          digest =
            :crypto.hash_update(digest, [
              <<byte_size(relative)::unsigned-big-64>>,
              relative,
              <<byte_size(bytes)::unsigned-big-64>>,
              bytes
            ])

          {:cont, {:ok, digest}}

        {:error, reason} ->
          {:halt, {:error, {:compiled_source_read_failed, path, reason}}}
      end
    end)
    |> case do
      {:ok, digest} -> {:ok, :crypto.hash_final(digest)}
      {:error, _reason} = error -> error
    end
  end
end
