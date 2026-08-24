defmodule FavnAuthoring.Deployment.ManifestArchive do
  @moduledoc false

  alias Favn.Manifest.ArchiveLimits

  @gzip_header_bytes 24
  @tar_block_bytes 512
  @file_mode 0o644

  @type result :: %{path: Path.t(), sha256: String.t(), status: :built | :already_built}

  @spec write(Path.t(), Path.t()) :: {:ok, result()} | {:error, term()}
  def write(directory, archive_path) when is_binary(directory) and is_binary(archive_path) do
    limits = ArchiveLimits.current()

    with {:ok, entries} <- entries(directory, limits),
         :ok <- File.mkdir_p(Path.dirname(archive_path)),
         {:ok, result} <- write_atomic(archive_path, entries, limits) do
      {:ok, result}
    end
  end

  defp entries(directory, limits) do
    bundle = Path.join(directory, "bundle.json")
    index = Path.join(directory, "manifest-index.json")

    packages =
      directory
      |> Path.join("execution-packages/*.json")
      |> Path.wildcard()
      |> Enum.sort()

    entries =
      [{"bundle.json", bundle}] ++
        Enum.map(packages, &{Path.relative_to(&1, directory), &1}) ++
        [{"manifest-index.json", index}]

    with true <- length(packages) <= limits.execution_packages,
         true <- length(entries) <= limits.tar_entries,
         :ok <- validate_entry(bundle, limits.bundle_bytes),
         :ok <- validate_entry(index, limits.manifest_index_bytes),
         :ok <- validate_packages(packages, limits.execution_package_bytes),
         true <- Enum.all?(entries, fn {path, _source} -> valid_archive_path?(path) end),
         expanded <- Enum.reduce(entries, @tar_block_bytes * 2, &expanded_entry_size/2),
         true <- expanded <= limits.expanded_bytes do
      {:ok, entries}
    else
      false -> {:error, :manifest_archive_limit_exceeded}
      {:error, _reason} = error -> error
    end
  rescue
    File.Error -> {:error, :manifest_archive_source_invalid}
  end

  defp validate_packages(paths, limit) do
    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case validate_entry(path, limit) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_entry(path, limit) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} when size <= limit -> :ok
      {:ok, %{type: :regular}} -> {:error, :manifest_archive_limit_exceeded}
      _other -> {:error, :manifest_archive_source_invalid}
    end
  end

  defp write_atomic(final_path, entries, limits) do
    temp_path = final_path <> ".tmp-#{System.unique_integer([:positive])}"

    try do
      with :ok <- stream_archive(temp_path, entries),
           {:ok, %{size: size}} <- File.stat(temp_path),
           true <- size <= limits.compressed_bytes,
           {:ok, sha256} <- file_sha256(temp_path),
           {:ok, status} <- install_archive(temp_path, final_path, sha256) do
        {:ok, %{path: final_path, sha256: sha256, status: status}}
      else
        false -> {:error, :manifest_archive_limit_exceeded}
        {:error, _reason} = error -> error
      end
    after
      _ = File.rm(temp_path)
    end
  end

  defp stream_archive(path, entries) do
    with {:ok, io} <- File.open(path, [:write, :binary, :exclusive]) do
      z = :zlib.open()

      try do
        :ok = :zlib.deflateInit(z, 6, :deflated, -15, 8, :default)
        :ok = IO.binwrite(io, gzip_header(0))
        state = %{io: io, z: z, crc: 0, size: 0}

        state =
          Enum.reduce(entries, state, fn {archive_path, source_path}, acc ->
            write_entry(acc, archive_path, source_path)
          end)

        state = write_tar_bytes(state, :binary.copy(<<0>>, @tar_block_bytes * 2))
        :ok = IO.binwrite(io, :zlib.deflate(z, <<>>, :finish))
        :ok = :zlib.deflateEnd(z)
        {:ok, deflate_end} = :file.position(io, :cur)
        deflate_bytes = deflate_end - @gzip_header_bytes
        {:ok, 0} = :file.position(io, 0)
        :ok = IO.binwrite(io, gzip_header(deflate_bytes))
        {:ok, ^deflate_end} = :file.position(io, :eof)
        :ok = IO.binwrite(io, <<state.crc::little-unsigned-32, state.size::little-unsigned-32>>)
        :ok = :file.sync(io)
        :ok
      rescue
        error -> {:error, {:manifest_archive_write_failed, error}}
      after
        :zlib.close(z)
        File.close(io)
      end
    end
  end

  defp write_entry(state, archive_path, source_path) do
    size = File.stat!(source_path).size
    state = write_tar_bytes(state, header(archive_path, size))

    state =
      source_path
      |> File.stream!(64 * 1_024, [])
      |> Enum.reduce(state, fn bytes, acc -> write_tar_bytes(acc, bytes) end)

    padding = padding(size)

    if padding == 0,
      do: state,
      else: write_tar_bytes(state, :binary.copy(<<0>>, padding))
  end

  defp write_tar_bytes(state, bytes) do
    :ok = IO.binwrite(state.io, :zlib.deflate(state.z, bytes))

    %{
      state
      | crc: :erlang.crc32(state.crc, bytes),
        size: Integer.mod(state.size + byte_size(bytes), 4_294_967_296)
    }
  end

  defp gzip_header(deflate_bytes) do
    <<0x1F, 0x8B, 8, 4, 0::little-unsigned-32, 0, 255, 12::little-unsigned-16, ?F, ?V,
      8::little-unsigned-16, deflate_bytes::little-unsigned-64>>
  end

  defp header(path, size) do
    name = pad(path, 100)
    mode = octal(@file_mode, 8)
    uid = octal(0, 8)
    gid = octal(0, 8)
    encoded_size = octal(size, 12)
    mtime = octal(0, 12)
    checksum_space = :binary.copy(" ", 8)

    unsigned =
      name <>
        mode <>
        uid <>
        gid <>
        encoded_size <>
        mtime <>
        checksum_space <>
        "0" <>
        :binary.copy(<<0>>, 100) <>
        "ustar\0" <>
        "00" <>
        :binary.copy(<<0>>, 32) <>
        :binary.copy(<<0>>, 32) <>
        octal(0, 8) <>
        octal(0, 8) <>
        :binary.copy(<<0>>, 155) <>
        :binary.copy(<<0>>, 12)

    checksum = unsigned |> :binary.bin_to_list() |> Enum.sum()
    checksum_field = checksum |> Integer.to_string(8) |> String.pad_leading(6, "0")

    binary_part(unsigned, 0, 148) <>
      checksum_field <> <<0, 32>> <> binary_part(unsigned, 156, 356)
  end

  defp octal(value, width) do
    encoded = Integer.to_string(value, 8)
    String.pad_leading(encoded, width - 1, "0") <> <<0>>
  end

  defp pad(value, width), do: value <> :binary.copy(<<0>>, width - byte_size(value))

  defp padding(size),
    do: Integer.mod(@tar_block_bytes - Integer.mod(size, @tar_block_bytes), @tar_block_bytes)

  defp expanded_entry_size({_path, source}, sum) do
    size = File.stat!(source).size
    sum + @tar_block_bytes + size + padding(size)
  end

  defp valid_archive_path?(path) do
    byte_size(path) in 1..100 and String.valid?(path) and Path.type(path) == :relative and
      not Enum.any?(Path.split(path), &(&1 in ["", ".", ".."])) and
      (path in ["bundle.json", "manifest-index.json"] or
         Regex.match?(~r/\Aexecution-packages\/[0-9a-f]{64}\.json\z/, path))
  end

  defp install_archive(temp_path, final_path, sha256) do
    if File.exists?(final_path) do
      case file_sha256(final_path) do
        {:ok, ^sha256} -> {:ok, :already_built}
        {:ok, _other} -> {:error, :manifest_archive_conflict}
        {:error, _reason} = error -> error
      end
    else
      case File.ln(temp_path, final_path) do
        :ok -> {:ok, :built}
        {:error, :eexist} -> install_archive(temp_path, final_path, sha256)
        {:error, reason} -> {:error, {:manifest_archive_install_failed, reason}}
      end
    end
  end

  defp file_sha256(path) do
    with {:ok, io} <- File.open(path, [:read, :binary]) do
      digest =
        io
        |> IO.binstream(64 * 1_024)
        |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
        |> :crypto.hash_final()
        |> Base.encode16(case: :lower)

      File.close(io)
      {:ok, digest}
    end
  end
end
