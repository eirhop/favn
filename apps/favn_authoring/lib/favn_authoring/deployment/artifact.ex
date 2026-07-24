defmodule FavnAuthoring.Deployment.Artifact do
  @moduledoc false

  alias Favn.Manifest.Serializer

  @bundle_schema_version 2

  @spec atomic_directory(Path.t(), (Path.t() -> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def atomic_directory(final_dir, build) when is_binary(final_dir) and is_function(build, 1) do
    parent = Path.dirname(final_dir)
    name = Path.basename(final_dir)
    temp_dir = Path.join(parent, ".#{name}.tmp-#{System.unique_integer([:positive])}")

    try do
      with :ok <- File.mkdir_p(parent),
           false <- File.exists?(final_dir),
           :ok <- File.mkdir(temp_dir) do
        case build.(temp_dir) do
          {:ok, result} ->
            case File.rename(temp_dir, final_dir) do
              :ok -> {:ok, result}
              {:error, reason} -> cleanup_error(temp_dir, {:artifact_rename_failed, reason})
            end

          {:error, reason} ->
            cleanup_error(temp_dir, reason)
        end
      else
        true -> {:error, :artifact_already_exists}
        {:error, reason} -> cleanup_error(temp_dir, reason)
      end
    rescue
      exception ->
        _ = File.rm_rf(temp_dir)
        reraise exception, __STACKTRACE__
    end
  end

  @spec write_json(Path.t(), map()) :: :ok | {:error, term()}
  def write_json(path, value) when is_map(value) do
    with {:ok, encoded} <- Serializer.encode_canonical(value),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, encoded <> "\n") do
      :ok
    end
  end

  @spec write_bundle(Path.t(), String.t(), map()) :: :ok | {:error, term()}
  def write_bundle(directory, kind, metadata \\ %{})
      when is_binary(directory) and is_binary(kind) and is_map(metadata) do
    files =
      directory
      |> Path.join("**/*")
      |> Path.wildcard(match_dot: true)
      |> Enum.filter(&File.regular?/1)
      |> Enum.reject(&(Path.relative_to(&1, directory) == "bundle.json"))
      |> Enum.map(fn path ->
        bytes = File.read!(path)

        %{
          "executable" => executable?(File.stat!(path).mode),
          "path" => Path.relative_to(path, directory),
          "sha256" => sha256(bytes),
          "size" => byte_size(bytes)
        }
      end)
      |> Enum.sort_by(& &1["path"])

    write_json(
      Path.join(directory, "bundle.json"),
      Map.merge(
        %{
          "schema_version" => @bundle_schema_version,
          "kind" => kind,
          "files" => files
        },
        metadata
      )
    )
  end

  @spec verify_bundle(Path.t(), String.t(), map()) :: :ok | {:error, term()}
  def verify_bundle(directory, kind, expected_metadata \\ %{})
      when is_binary(directory) and is_binary(kind) and is_map(expected_metadata) do
    with {:ok, bytes} <- File.read(Path.join(directory, "bundle.json")),
         {:ok, bundle} <- JSON.decode(bytes),
         @bundle_schema_version <- bundle["schema_version"],
         ^kind <- bundle["kind"],
         true <- metadata_matches?(bundle, expected_metadata),
         {:ok, declared} <- declared_files(bundle["files"]),
         {:ok, actual} <- actual_files(directory),
         true <- Map.keys(declared) |> Enum.sort() == Map.keys(actual) |> Enum.sort(),
         :ok <- verify_declared_files(declared, actual) do
      :ok
    else
      _invalid -> {:error, :artifact_bundle_invalid}
    end
  end

  defp metadata_matches?(bundle, expected) do
    Enum.all?(expected, fn {key, value} -> Map.get(bundle, key) == value end)
  end

  defp declared_files(files) when is_list(files) do
    files
    |> Enum.reduce_while({:ok, %{}}, fn
      %{"executable" => executable, "path" => path, "sha256" => digest, "size" => size},
      {:ok, acc}
      when is_boolean(executable) and is_binary(path) and is_binary(digest) and is_integer(size) and
             size >= 0 ->
        if safe_relative_path?(path) and valid_digest?(digest) and not Map.has_key?(acc, path) do
          {:cont,
           {:ok, Map.put(acc, path, %{executable: executable, sha256: digest, size: size})}}
        else
          {:halt, {:error, :invalid_bundle_file}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_bundle_file}}
    end)
  end

  defp declared_files(_files), do: {:error, :invalid_bundle_files}

  defp actual_files(directory) do
    directory
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, acc} ->
      relative = Path.relative_to(path, directory)

      case File.lstat(path) do
        {:ok, %{type: :directory}} ->
          {:cont, {:ok, acc}}

        {:ok, %{type: :regular}} when relative == "bundle.json" ->
          {:cont, {:ok, acc}}

        {:ok, %{type: :regular}} ->
          bytes = File.read!(path)
          executable = executable?(File.stat!(path).mode)

          {:cont,
           {:ok,
            Map.put(acc, relative, %{
              executable: executable,
              sha256: sha256(bytes),
              size: byte_size(bytes)
            })}}

        _unsupported ->
          {:halt, {:error, :unsupported_artifact_entry}}
      end
    end)
  end

  defp verify_declared_files(declared, actual) do
    if declared == actual, do: :ok, else: {:error, :artifact_file_mismatch}
  end

  defp safe_relative_path?(path) do
    path != "" and Path.type(path) == :relative and
      not Enum.any?(Path.split(path), &(&1 in ["", ".", ".."]))
  end

  defp valid_digest?(digest) do
    byte_size(digest) == 64 and digest =~ ~r/\A[0-9a-f]{64}\z/
  end

  defp executable?(mode), do: Bitwise.band(mode, 0o111) != 0

  defp cleanup_error(temp_dir, reason) do
    _ = File.rm_rf(temp_dir)
    {:error, reason}
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
