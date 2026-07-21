defmodule Favn.Dev.Build.Artifact do
  @moduledoc false

  alias Favn.Manifest.Serializer

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
          "schema_version" => 1,
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
         1 <- bundle["schema_version"],
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

  @spec copy_tree(Path.t(), Path.t()) :: :ok | {:error, term()}
  def copy_tree(source, destination) do
    if File.dir?(source) do
      with :ok <- validate_source_tree(source),
           :ok <- File.mkdir_p(destination) do
        source
        |> Path.join("**/*")
        |> Path.wildcard(match_dot: true)
        |> Enum.reject(&ignored_path?(source, &1))
        |> Enum.reduce_while(:ok, fn path, :ok ->
          relative = Path.relative_to(path, source)
          target = Path.join(destination, relative)

          case File.lstat(path) do
            {:ok, %{type: :directory}} ->
              continue(File.mkdir_p(target))

            {:ok, %{type: :regular}} ->
              with :ok <- File.mkdir_p(Path.dirname(target)), do: continue(File.cp(path, target))

            {:ok, %{type: :symlink}} ->
              {:halt, {:error, {:symlink_not_supported, relative}}}

            {:ok, _other} ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {:source_copy_failed, relative, reason}}}
          end
        end)
      end
    else
      {:error, {:source_directory_missing, Path.basename(source)}}
    end
  end

  defp validate_source_tree(source) do
    source
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reject(&ignored_path?(source, &1))
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case File.lstat(path) do
        {:ok, %{type: :regular}} ->
          relative = Path.relative_to(path, source)

          if sensitive_source_file?(relative, path) do
            {:halt, {:error, {:sensitive_source_file, relative}}}
          else
            {:cont, :ok}
          end

        _other ->
          {:cont, :ok}
      end
    end)
  end

  defp sensitive_source_file?(relative, path) do
    components = relative |> Path.split() |> Enum.map(&String.downcase/1)
    basename = List.last(components)
    extension = Path.extname(basename)

    sensitive_name =
      basename in [
        ".env",
        ".npmrc",
        ".netrc",
        "credentials",
        "credentials.json",
        "service-account.json",
        "service_account.json",
        "id_rsa",
        "id_ed25519"
      ] or
        (String.starts_with?(basename, ".env.") and
           basename not in [".env.example", ".env.sample", ".env.template"]) or
        extension in [".key", ".p12", ".pfx"] or
        Enum.any?(components, &(&1 in [".aws", ".azure", ".kube"]))

    sensitive_name or contains_private_key?(path)
  end

  defp contains_private_key?(path) do
    case File.open(path, [:read, :binary], fn device -> IO.binread(device, 1_048_576) end) do
      {:ok, bytes} when is_binary(bytes) ->
        prefix = "-----BEGIN "
        suffix = " KEY-----"

        Enum.any?(
          Enum.map(["PRIVATE", "RSA PRIVATE", "EC PRIVATE", "OPENSSH PRIVATE"], fn kind ->
            prefix <> kind <> suffix
          end),
          &(:binary.match(bytes, &1) != :nomatch)
        )

      {:ok, :eof} ->
        false

      {:error, _reason} ->
        true
    end
  end

  defp ignored_path?(source, path) do
    case path |> Path.relative_to(source) |> Path.split() do
      [top | _rest] -> top in [".git", ".favn", "_build", "deps", "test", "doc", "docs"]
      [] -> false
    end
  end

  defp metadata_matches?(bundle, expected) do
    Enum.all?(expected, fn {key, value} -> Map.get(bundle, key) == value end)
  end

  defp declared_files(files) when is_list(files) do
    files
    |> Enum.reduce_while({:ok, %{}}, fn
      %{"path" => path, "sha256" => digest, "size" => size}, {:ok, acc}
      when is_binary(path) and is_binary(digest) and is_integer(size) and size >= 0 ->
        if safe_relative_path?(path) and valid_digest?(digest) and not Map.has_key?(acc, path) do
          {:cont, {:ok, Map.put(acc, path, %{sha256: digest, size: size})}}
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

          {:cont, {:ok, Map.put(acc, relative, %{sha256: sha256(bytes), size: byte_size(bytes)})}}

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

  defp continue(:ok), do: {:cont, :ok}
  defp continue({:error, reason}), do: {:halt, {:error, reason}}

  defp cleanup_error(temp_dir, reason) do
    _ = File.rm_rf(temp_dir)
    {:error, reason}
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
