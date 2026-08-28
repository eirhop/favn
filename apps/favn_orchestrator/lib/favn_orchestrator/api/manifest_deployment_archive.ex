defmodule FavnOrchestrator.API.ManifestDeploymentArchive do
  @moduledoc false

  alias Favn.Manifest.ArchiveLimits
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.BoundedWorker
  alias FavnOrchestrator.MemoryCapacity.Budget

  @gzip_header_bytes 24
  @tar_block_bytes 512
  @gzip_footer_bytes 8

  defstruct [
    :z,
    :persist_packages,
    :bundle,
    :version,
    :current,
    :started_at_ms,
    :clock,
    :upload_timeout_ms,
    :capacity_token,
    compressed_hash: nil,
    compressed_bytes: 0,
    expanded_bytes: 0,
    expanded_crc: 0,
    gzip_header: <<>>,
    gzip_pending: <<>>,
    deflate_remaining: nil,
    tar_buffer: <<>>,
    tar_finished?: false,
    zero_blocks: 0,
    entries: 0,
    paths: MapSet.new(),
    package_paths: [],
    package_hashes: MapSet.new(),
    package_batch: [],
    package_batch_bytes: 0,
    package_batch_count_limit: nil,
    package_batch_bytes_limit: nil
  ]

  @type t :: %__MODULE__{}

  @spec new(([ExecutionPackage.t()] -> :ok | {:error, term()}), keyword()) :: t()
  def new(persist_packages, opts \\ []) when is_function(persist_packages, 1) do
    z = :zlib.open()
    :ok = :zlib.inflateInit(z, -15)
    limits = ArchiveLimits.current()
    clock = Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end)

    %__MODULE__{
      z: z,
      persist_packages: persist_packages,
      compressed_hash: :crypto.hash_init(:sha256),
      clock: clock,
      started_at_ms: Keyword.get(opts, :started_at_ms, clock.()),
      upload_timeout_ms: Keyword.get(opts, :upload_timeout_ms, limits.upload_timeout_ms),
      capacity_token: Keyword.get(opts, :capacity_token),
      package_batch_count_limit:
        Keyword.get(opts, :package_batch_count, limits.package_batch_count),
      package_batch_bytes_limit:
        Keyword.get(opts, :package_batch_bytes, limits.package_batch_bytes)
    }
  end

  @spec feed(t(), binary()) :: {:ok, t()} | {:error, term()}
  def feed(%__MODULE__{} = state, bytes) when is_binary(bytes) do
    limits = ArchiveLimits.current()
    compressed_bytes = state.compressed_bytes + byte_size(bytes)

    cond do
      compressed_bytes > limits.compressed_bytes ->
        {:error, :compressed_limit_exceeded}

      state.clock.() - state.started_at_ms > state.upload_timeout_ms ->
        {:error, :upload_timeout}

      true ->
        state = %{
          state
          | compressed_bytes: compressed_bytes,
            compressed_hash: :crypto.hash_update(state.compressed_hash, bytes)
        }

        with {:ok, state} <- feed_gzip(state, bytes),
             :ok <- validate_deadline(state) do
          {:ok, state}
        end
    end
  rescue
    _error -> {:error, :malformed_gzip}
  catch
    _kind, _reason -> {:error, :malformed_gzip}
  end

  @spec finish(t()) :: {:ok, map()} | {:error, term()}
  def finish(%__MODULE__{} = state) do
    with :ok <- validate_deadline(state),
         true <- byte_size(state.gzip_header) == @gzip_header_bytes,
         true <- state.deflate_remaining == 0,
         true <- byte_size(state.gzip_pending) == @gzip_footer_bytes,
         {:ok, state} <- finish_inflate(state),
         :ok <- validate_gzip_footer(state),
         true <- state.tar_finished?,
         true <- state.current == nil and state.tar_buffer == <<>>,
         true <- state.zero_blocks == 2,
         %Version{} = version <- state.version,
         :ok <- validate_package_inventory(state, version),
         {:ok, state} <- flush_packages(state),
         :ok <- validate_deadline(state) do
      close(state)

      {:ok,
       %{
         version: version,
         archive_sha256:
           state.compressed_hash |> :crypto.hash_final() |> Base.encode16(case: :lower),
         compressed_bytes: state.compressed_bytes,
         expanded_bytes: state.expanded_bytes,
         entry_count: state.entries,
         package_count: MapSet.size(state.package_hashes)
       }}
    else
      false -> close_error(state, :invalid_manifest_archive)
      nil -> close_error(state, :invalid_manifest_archive)
      {:error, _reason} = error -> close_error(state, error)
    end
  rescue
    _error -> close_error(state, :malformed_gzip)
  catch
    _kind, _reason -> close_error(state, :malformed_gzip)
  end

  @doc false
  @spec discard(t()) :: :ok
  def discard(%__MODULE__{} = state), do: close(state)

  defp feed_gzip(state, <<>>), do: {:ok, state}

  defp feed_gzip(state, bytes) when byte_size(state.gzip_header) < @gzip_header_bytes do
    needed = @gzip_header_bytes - byte_size(state.gzip_header)
    take = min(needed, byte_size(bytes))
    <<header_part::binary-size(^take), rest::binary>> = bytes
    header = state.gzip_header <> header_part

    if byte_size(header) < @gzip_header_bytes do
      {:ok, %{state | gzip_header: header}}
    else
      case parse_gzip_header(header) do
        {:ok, deflate_bytes} ->
          feed_gzip(%{state | gzip_header: header, deflate_remaining: deflate_bytes}, rest)

        :error ->
          {:error, :invalid_gzip_header}
      end
    end
  end

  defp feed_gzip(%{deflate_remaining: remaining} = state, bytes) when remaining > 0 do
    take = min(remaining, byte_size(bytes))
    <<inflate_bytes::binary-size(^take), rest::binary>> = bytes

    with {:ok, state} <- inflate(%{state | deflate_remaining: remaining - take}, inflate_bytes) do
      feed_gzip(state, rest)
    end
  end

  defp feed_gzip(state, bytes) do
    pending = state.gzip_pending <> bytes

    if byte_size(pending) <= @gzip_footer_bytes do
      {:ok, %{state | gzip_pending: pending}}
    else
      {:error, :trailing_compressed_data}
    end
  end

  defp parse_gzip_header(
         <<0x1F, 0x8B, 8, 4, 0::little-unsigned-32, 0, 255, 12::little-unsigned-16, ?F, ?V,
           8::little-unsigned-16, deflate_bytes::little-unsigned-64>>
       ) do
    max_deflate_bytes =
      ArchiveLimits.current().compressed_bytes - @gzip_header_bytes - @gzip_footer_bytes

    if deflate_bytes > 0 and deflate_bytes <= max_deflate_bytes,
      do: {:ok, deflate_bytes},
      else: :error
  end

  defp parse_gzip_header(_header), do: :error

  defp inflate(state, <<>>), do: {:ok, state}

  defp inflate(state, bytes) do
    inflate_result(state, :zlib.safeInflate(state.z, bytes))
  end

  defp inflate_result(state, {status, output}) when status in [:continue, :finished] do
    with {:ok, state} <- consume_inflated(state, IO.iodata_to_binary(output)) do
      case status do
        :continue when output == [] or output == <<>> -> {:ok, state}
        :continue -> inflate_result(state, :zlib.safeInflate(state.z, <<>>))
        :finished -> {:ok, state}
      end
    end
  end

  defp inflate_result(_state, _other), do: {:error, :malformed_gzip}

  defp finish_inflate(state) do
    with {:ok, state} <- inflate_result(state, :zlib.safeInflate(state.z, <<>>)),
         :ok <- :zlib.inflateEnd(state.z) do
      {:ok, state}
    else
      _invalid -> {:error, :malformed_gzip}
    end
  end

  defp consume_inflated(state, <<>>), do: {:ok, state}

  defp consume_inflated(state, bytes) do
    limits = ArchiveLimits.current()
    expanded = state.expanded_bytes + byte_size(bytes)

    if expanded > limits.expanded_bytes do
      {:error, :expanded_limit_exceeded}
    else
      state = %{
        state
        | expanded_bytes: expanded,
          expanded_crc: :erlang.crc32(state.expanded_crc, bytes)
      }

      consume_tar(%{state | tar_buffer: state.tar_buffer <> bytes})
    end
  end

  defp consume_tar(%{tar_finished?: true, tar_buffer: buffer} = state) do
    if all_zero?(buffer),
      do: {:ok, %{state | tar_buffer: <<>>}},
      else: {:error, :tar_trailing_data}
  end

  defp consume_tar(%{current: nil, tar_buffer: buffer} = state)
       when byte_size(buffer) >= @tar_block_bytes do
    <<header::binary-size(@tar_block_bytes), rest::binary>> = buffer
    state = %{state | tar_buffer: rest}

    if all_zero?(header) do
      zero_blocks = state.zero_blocks + 1

      cond do
        zero_blocks == 1 -> consume_tar(%{state | zero_blocks: zero_blocks})
        zero_blocks == 2 -> consume_tar(%{state | zero_blocks: zero_blocks, tar_finished?: true})
        true -> {:error, :invalid_tar_terminator}
      end
    else
      with true <- state.zero_blocks == 0,
           {:ok, path, size} <- parse_header(header),
           :ok <- validate_next_path(state, path),
           :ok <- validate_entry_limit(path, size),
           :ok <- reserve_entry(state, path, size),
           :ok <- validate_tar_entry_count(state) do
        current = %{
          path: path,
          size: size,
          remaining: size,
          padding: padding(size),
          bytes: [],
          body_size: 0
        }

        consume_tar(%{
          state
          | current: current,
            entries: state.entries + 1,
            paths: MapSet.put(state.paths, path)
        })
      else
        false -> {:error, :invalid_tar_header}
        {:error, _reason} = error -> error
      end
    end
  end

  defp consume_tar(%{current: nil} = state), do: {:ok, state}

  defp consume_tar(%{current: current, tar_buffer: buffer} = state) do
    body_take = min(current.remaining, byte_size(buffer))
    <<body::binary-size(^body_take), rest::binary>> = buffer

    current = %{
      current
      | remaining: current.remaining - body_take,
        bytes: if(body_take == 0, do: current.bytes, else: [body | current.bytes]),
        body_size: current.body_size + body_take
    }

    cond do
      current.remaining > 0 ->
        {:ok, %{state | current: current, tar_buffer: rest}}

      byte_size(rest) < current.padding ->
        {:ok, %{state | current: current, tar_buffer: rest}}

      true ->
        pad_size = current.padding
        <<pad::binary-size(^pad_size), tail::binary>> = rest

        if all_zero?(pad) do
          bytes = current.bytes |> Enum.reverse() |> IO.iodata_to_binary()

          with {:ok, state} <-
                 process_entry(%{state | current: nil, tar_buffer: tail}, current.path, bytes) do
            consume_tar(state)
          end
        else
          {:error, :invalid_tar_padding}
        end
    end
  end

  defp parse_header(header) do
    <<name::binary-size(100), mode::binary-size(8), uid::binary-size(8), gid::binary-size(8),
      size::binary-size(12), mtime::binary-size(12), checksum::binary-size(8),
      type::binary-size(1), link::binary-size(100), magic::binary-size(6),
      version::binary-size(2), uname::binary-size(32), gname::binary-size(32),
      major::binary-size(8), minor::binary-size(8), prefix::binary-size(155),
      trailer::binary-size(12)>> = header

    with {:ok, path} <- nul_string(name),
         {:ok, 0o644} <- octal(mode),
         {:ok, 0} <- octal(uid),
         {:ok, 0} <- octal(gid),
         {:ok, file_size} <- octal(size),
         {:ok, 0} <- octal(mtime),
         true <- valid_checksum?(header, checksum),
         true <- type == "0" and all_zero?(link),
         true <- magic == "ustar\0" and version == "00",
         true <- all_zero?(uname) and all_zero?(gname),
         {:ok, 0} <- octal(major),
         {:ok, 0} <- octal(minor),
         true <- all_zero?(prefix) and all_zero?(trailer) do
      {:ok, path, file_size}
    else
      _invalid -> {:error, :invalid_tar_header}
    end
  end

  defp valid_checksum?(header, checksum) do
    with {:ok, expected} <- checksum_octal(checksum) do
      unsigned =
        binary_part(header, 0, 148) <> :binary.copy(" ", 8) <> binary_part(header, 156, 356)

      expected == unsigned |> :binary.bin_to_list() |> Enum.sum()
    else
      _invalid -> false
    end
  end

  defp validate_next_path(state, path) do
    cond do
      MapSet.member?(state.paths, path) -> {:error, :duplicate_tar_path}
      state.entries == 0 and path != "bundle.json" -> {:error, :bundle_must_be_first}
      is_nil(state.bundle) and path != "bundle.json" -> {:error, :bundle_must_be_first}
      path == "bundle.json" and state.entries > 0 -> {:error, :duplicate_tar_path}
      path == "bundle.json" -> :ok
      not is_nil(state.version) -> {:error, :manifest_index_must_be_last}
      path == "manifest-index.json" -> :ok
      package_path?(path) -> validate_package_order(state.package_paths, path)
      true -> {:error, {:unsupported_tar_path, path}}
    end
  end

  defp validate_package_order([], _path), do: :ok

  defp validate_package_order([previous | _rest], path) do
    if previous < path, do: :ok, else: {:error, :packages_not_sorted}
  end

  defp validate_entry_limit("bundle.json", size) do
    if size <= ArchiveLimits.current().bundle_bytes,
      do: :ok,
      else: {:error, :bundle_limit_exceeded}
  end

  defp validate_entry_limit("manifest-index.json", size) do
    if size <= ArchiveLimits.current().manifest_index_bytes,
      do: :ok,
      else: {:error, :manifest_index_limit_exceeded}
  end

  defp validate_entry_limit(path, size) do
    if package_path?(path) and size <= ArchiveLimits.current().execution_package_bytes,
      do: :ok,
      else: {:error, :execution_package_limit_exceeded}
  end

  defp validate_tar_entry_count(state) do
    if state.entries + 1 <= ArchiveLimits.current().tar_entries,
      do: :ok,
      else: {:error, :tar_entry_limit_exceeded}
  end

  defp process_entry(state, "bundle.json", bytes) do
    with {:ok, normalized} <-
           BoundedWorker.run_serialized(
             fn ->
               with {:ok, bundle} when is_map(bundle) <- Jason.decode(bytes),
                    {:ok, normalized} <- validate_bundle(bundle) do
                 {:ok, normalized}
               else
                 _invalid -> {:error, :invalid_bundle}
               end
             end,
             Budget.manifest_base(),
             Budget.serialized_result_limit(Budget.manifest_base())
           ),
         :ok <- preflight_declared_capacity(state, normalized) do
      {:ok, %{state | bundle: normalized}}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_bundle}
    end
  end

  defp process_entry(state, "manifest-index.json" = path, bytes) do
    budget = Budget.index(byte_size(bytes))

    with {:ok, version} <-
           BoundedWorker.run_serialized(
             fn ->
               with :ok <- verify_declared_file(state.bundle, path, bytes),
                    {:ok, manifest} <- Serializer.decode_manifest(bytes),
                    {:ok, version} <-
                      Version.from_published(manifest, version_options(state.bundle)) do
                 {:ok, version}
               end
             end,
             budget,
             Budget.serialized_result_limit(budget)
           ) do
      {:ok, %{state | version: version}}
    else
      {:error, _reason} = error -> error
    end
  end

  defp process_entry(state, path, bytes) do
    hash = path |> Path.basename(".json")

    with :ok <- validate_package_count(state),
         {:ok, package} <-
           BoundedWorker.run_serialized(
             fn ->
               with :ok <- verify_declared_file(state.bundle, path, bytes),
                    {:ok, decoded} when is_map(decoded) <- Jason.decode(bytes),
                    {:ok, package} <- ExecutionPackage.from_published(decoded),
                    true <- package.content_hash == hash do
                 {:ok, package}
               else
                 false -> {:error, :invalid_execution_package_identity}
                 {:error, _reason} = error -> error
                 _invalid -> {:error, :invalid_execution_package}
               end
             end,
             Budget.manifest_base(),
             Budget.serialized_result_limit(Budget.manifest_base())
           ),
         {:ok, state} <- flush_before_package(state, byte_size(bytes)) do
      state = %{
        state
        | package_paths: [path | state.package_paths],
          package_hashes: MapSet.put(state.package_hashes, hash),
          package_batch: [package | state.package_batch],
          package_batch_bytes: state.package_batch_bytes + byte_size(bytes)
      }

      maybe_flush_packages(state)
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_execution_package}
    end
  end

  defp validate_package_count(state) do
    if MapSet.size(state.package_hashes) < ArchiveLimits.current().execution_packages,
      do: :ok,
      else: {:error, :execution_package_count_exceeded}
  end

  defp validate_bundle(
         %{
           "schema_version" => 2,
           "kind" => "favn_manifest_release",
           "files" => files,
           "manifest" => manifest
         } = bundle
       )
       when is_list(files) and is_map(manifest) do
    with true <- Map.keys(bundle) |> Enum.sort() == ~w(files kind manifest schema_version),
         true <-
           Map.keys(manifest) |> Enum.sort() ==
             ~w(content_hash execution_packages_path index_path manifest_version_id runner_contract_version runner_releases schema_version serialization_format),
         true <- manifest["index_path"] == "manifest-index.json",
         true <- manifest["execution_packages_path"] == "execution-packages",
         {:ok, declared} <- declared_files(files),
         true <- Map.has_key?(declared, "manifest-index.json") do
      {:ok, %{"manifest" => manifest, "files" => declared}}
    else
      false -> {:error, :invalid_bundle}
      {:error, _reason} = error -> error
    end
  end

  defp validate_bundle(_bundle), do: {:error, :invalid_bundle}

  defp declared_files(files) do
    if length(files) <= ArchiveLimits.current().execution_packages + 1 do
      Enum.reduce_while(files, {:ok, %{}}, fn
        %{"executable" => false, "path" => path, "sha256" => sha, "size" => size} = file,
        {:ok, acc}
        when is_binary(path) and is_binary(sha) and is_integer(size) and size >= 0 ->
          with true <- Map.keys(file) |> Enum.sort() == ~w(executable path sha256 size),
               true <- path == "manifest-index.json" or package_path?(path),
               true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, sha),
               false <- Map.has_key?(acc, path),
               :ok <- validate_entry_limit(path, size) do
            {:cont, {:ok, Map.put(acc, path, %{sha256: sha, size: size})}}
          else
            {:error, reason} -> {:halt, {:error, reason}}
            _invalid -> {:halt, {:error, :invalid_bundle_file}}
          end

        _entry, _acc ->
          {:halt, {:error, :invalid_bundle_file}}
      end)
    else
      {:error, :invalid_bundle_file}
    end
  end

  defp preflight_declared_capacity(%{capacity_token: nil}, _bundle), do: :ok

  defp preflight_declared_capacity(state, %{"files" => files}) do
    largest_budget =
      files
      |> Enum.map(fn
        {"manifest-index.json", %{size: size}} -> Budget.index(size)
        {_path, _metadata} -> Budget.manifest_base()
      end)
      |> Enum.max(fn -> Budget.manifest_base() end)

    with :ok <- MemoryCapacity.resize(state.capacity_token, largest_budget) do
      MemoryCapacity.resize(state.capacity_token, Budget.manifest_base())
    end
  end

  defp verify_declared_file(%{"files" => declared}, path, bytes) do
    sha = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

    case Map.get(declared, path) do
      %{sha256: ^sha, size: size} when size == byte_size(bytes) -> :ok
      _missing_or_mismatch -> {:error, :bundle_file_mismatch}
    end
  end

  defp version_options(%{"manifest" => manifest}) do
    [
      manifest_version_id: manifest["manifest_version_id"],
      content_hash: manifest["content_hash"],
      schema_version: manifest["schema_version"],
      runner_contract_version: manifest["runner_contract_version"],
      runner_releases: manifest["runner_releases"],
      serialization_format: manifest["serialization_format"]
    ]
  end

  defp validate_package_inventory(state, version) do
    declared_paths =
      state.bundle["files"]
      |> Map.keys()
      |> Enum.filter(&package_path?/1)
      |> MapSet.new()

    seen_paths = state.package_paths |> MapSet.new()
    required_hashes = version |> Publication.required_package_hashes() |> MapSet.new()

    if declared_paths == seen_paths and required_hashes == state.package_hashes,
      do: :ok,
      else: {:error, :execution_package_inventory_mismatch}
  end

  defp maybe_flush_packages(state) do
    if length(state.package_batch) >= state.package_batch_count_limit or
         state.package_batch_bytes >= state.package_batch_bytes_limit do
      flush_packages(state)
    else
      {:ok, state}
    end
  end

  defp flush_before_package(%{package_batch: []} = state, _next_bytes), do: {:ok, state}

  defp flush_before_package(state, next_bytes) do
    if length(state.package_batch) + 1 > state.package_batch_count_limit or
         state.package_batch_bytes + next_bytes > state.package_batch_bytes_limit do
      flush_packages(state)
    else
      {:ok, state}
    end
  end

  defp flush_packages(%{package_batch: []} = state), do: {:ok, state}

  defp flush_packages(state) do
    packages = Enum.reverse(state.package_batch)

    with :ok <- reserve_working(state, Budget.manifest_base()) do
      case state.persist_packages.(packages) do
        :ok -> {:ok, %{state | package_batch: [], package_batch_bytes: 0}}
        {:error, reason} -> {:error, {:package_persistence_failed, reason}}
        _invalid -> {:error, :package_persistence_failed}
      end
    end
  end

  defp reserve_entry(state, "manifest-index.json", size),
    do: reserve_working(state, Budget.index(size))

  defp reserve_entry(state, _path, _size),
    do: reserve_working(state, Budget.manifest_base())

  defp reserve_working(%{capacity_token: nil}, _bytes), do: :ok
  defp reserve_working(%{capacity_token: token}, bytes), do: MemoryCapacity.resize(token, bytes)

  defp validate_gzip_footer(state) do
    <<expected_crc::little-unsigned-32, expected_size::little-unsigned-32>> = state.gzip_pending

    if expected_crc == state.expanded_crc and
         expected_size == Integer.mod(state.expanded_bytes, 4_294_967_296) do
      :ok
    else
      {:error, :invalid_gzip_footer}
    end
  end

  defp validate_deadline(state) do
    if state.clock.() - state.started_at_ms <= state.upload_timeout_ms,
      do: :ok,
      else: {:error, :upload_timeout}
  end

  defp nul_string(field) do
    case :binary.split(field, <<0>>) do
      [value, rest] when value != "" ->
        if all_zero?(rest) and String.valid?(value), do: {:ok, value}, else: :error

      _invalid ->
        :error
    end
  end

  defp octal(field) do
    width = byte_size(field) - 1

    with true <- byte_size(field) > 1,
         <<digits::binary-size(^width), 0>> <- field,
         true <- digits != "" and Regex.match?(~r/\A[0-7]+\z/, digits),
         {value, ""} <- Integer.parse(digits, 8) do
      {:ok, value}
    else
      _invalid -> :error
    end
  end

  defp checksum_octal(<<digits::binary-size(6), 0, 32>>) do
    if Regex.match?(~r/\A[0-7]{6}\z/, digits) do
      case Integer.parse(digits, 8) do
        {value, ""} -> {:ok, value}
        _invalid -> :error
      end
    else
      :error
    end
  end

  defp checksum_octal(_field), do: :error

  defp package_path?(path),
    do: Regex.match?(~r/\Aexecution-packages\/[0-9a-f]{64}\.json\z/, path)

  defp all_zero?(bytes), do: bytes == :binary.copy(<<0>>, byte_size(bytes))

  defp padding(size),
    do: Integer.mod(@tar_block_bytes - Integer.mod(size, @tar_block_bytes), @tar_block_bytes)

  defp close_error(state, {:error, _reason} = error) do
    close(state)
    error
  end

  defp close_error(state, reason) do
    close(state)
    {:error, reason}
  end

  defp close(state) do
    close_zlib(state.z)
    :ok
  end

  defp close_zlib(z) do
    try do
      :zlib.close(z)
    rescue
      _error -> :ok
    end
  end
end
