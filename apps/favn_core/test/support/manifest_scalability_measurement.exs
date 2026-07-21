defmodule FavnTestSupport.ManifestScalabilityMeasurement do
  @moduledoc """
  Measures the current manifest representation with bounded, isolated process samples.

  Each sample runs in its own process. The caller samples that process while it
  builds, versions, serializes, compresses, hashes, decodes, and attributes the
  compact current-schema index. This module is deliberately test-only and is not a
  production API.
  """

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnTestSupport.ManifestScalabilityFixture

  @default_sample_interval_ms 100
  @binary_sample_interval_ms 1_000
  @default_timeout_ms 1_200_000
  @allowed_opts [:sql_columns, :contract_columns, :sample_interval_ms, :timeout_ms, :progress]

  @type option ::
          ManifestScalabilityFixture.option()
          | {:sample_interval_ms, pos_integer()}
          | {:timeout_ms, pos_integer()}
          | {:progress, boolean()}

  @type process_sample :: %{
          process_memory_bytes: non_neg_integer(),
          referenced_binary_bytes: non_neg_integer()
        }

  @doc "Measures one SQL-heavy manifest and returns a JSON-safe report."
  @spec measure(pos_integer(), [option()]) :: map()
  def measure(asset_count, opts \\ [])

  def measure(asset_count, opts) when is_list(opts) do
    config = validate_options!(opts)
    parent = self()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        run_worker(parent, asset_count, config)
      end)

    deadline_ms = System.monotonic_time(:millisecond) + config.timeout_ms

    collect(pid, monitor_ref, deadline_ms, config.sample_interval_ms, config.progress, %{})
  end

  def measure(_asset_count, opts) do
    raise ArgumentError,
          "manifest scalability measurement options must be a keyword list, got: #{inspect(opts)}"
  end

  defp run_worker(parent, asset_count, config) do
    try do
      fixture_opts = [
        sql_columns: config.sql_columns,
        contract_columns: config.contract_columns
      ]

      manifest =
        phase(parent, :build, fn ->
          ManifestScalabilityFixture.build(asset_count, fixture_opts)
        end)

      version_hash =
        phase(parent, :version, fn ->
          {:ok, version} = Version.new(manifest, manifest_version_id: "scalability-measurement")
          version.content_hash
        end)

      encoded = phase(parent, :encode, fn -> Serializer.encode_manifest!(manifest) end)
      gzip = phase(parent, :gzip, fn -> :zlib.gzip(encoded) end)

      raw_hash =
        phase(parent, :sha256, fn ->
          :crypto.hash(:sha256, encoded)
          |> Base.encode16(case: :lower)
        end)

      if raw_hash != version_hash do
        raise "version hash did not match the independently encoded manifest"
      end

      decoded = phase(parent, :decode, fn -> Jason.decode!(encoded) end)

      sizes = phase(parent, :sizes, fn -> sizes(encoded, gzip, decoded, asset_count) end)
      attribution = phase(parent, :attribute, fn -> attribution(decoded) end)

      send(parent, {
        :measurement_complete,
        self(),
        %{
          fixture_version: 2,
          sample_count: 1,
          asset_count: asset_count,
          fixture: %{
            sql_columns: config.sql_columns,
            contract_columns: config.contract_columns
          },
          sampling: %{
            interval_ms: config.sample_interval_ms,
            referenced_binary_interval_ms: @binary_sample_interval_ms,
            memory_scope: "worker process and its referenced binaries; not operating-system RSS"
          },
          environment: environment(),
          hashes: %{canonical_sha256: raw_hash},
          sizes: sizes,
          attribution: attribution
        }
      })
    rescue
      error ->
        send(parent, {
          :measurement_failed,
          self(),
          Exception.format(:error, error, __STACKTRACE__)
        })
    catch
      kind, reason ->
        send(parent, {
          :measurement_failed,
          self(),
          Exception.format(kind, reason, __STACKTRACE__)
        })
    end
  end

  defp phase(parent, name, function) do
    send(parent, {:measurement_phase_start, self(), name})

    receive do
      {:measurement_phase_ready, ^name} -> :ok
    end

    started_at = System.monotonic_time(:microsecond)
    result = function.()
    duration_us = System.monotonic_time(:microsecond) - started_at

    send(parent, {:measurement_phase_finish, self(), name, duration_us})

    receive do
      {:measurement_phase_recorded, ^name} -> :ok
    end

    result
  end

  defp collect(pid, monitor_ref, deadline_ms, sample_interval_ms, progress?, operations) do
    timeout_ms = remaining_timeout!(pid, deadline_ms)

    receive do
      {:measurement_phase_start, ^pid, name} ->
        if progress?, do: IO.puts(:stderr, "  phase: #{name}")
        baseline = process_sample(pid)
        send(pid, {:measurement_phase_ready, name})

        {operation, operations} =
          collect_phase(
            pid,
            monitor_ref,
            name,
            deadline_ms,
            sample_interval_ms,
            System.monotonic_time(:millisecond),
            baseline,
            baseline,
            operations
          )

        collect(
          pid,
          monitor_ref,
          deadline_ms,
          sample_interval_ms,
          progress?,
          Map.put(operations, name, operation)
        )

      {:measurement_complete, ^pid, report} ->
        Process.demonitor(monitor_ref, [:flush])
        Map.put(report, :operations, operations)

      {:measurement_failed, ^pid, formatted_error} ->
        Process.demonitor(monitor_ref, [:flush])
        raise "manifest scalability measurement failed:\n#{formatted_error}"

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        raise "manifest scalability measurement process exited: #{inspect(reason)}"
    after
      timeout_ms -> timeout!(pid)
    end
  end

  defp collect_phase(
         pid,
         monitor_ref,
         name,
         deadline_ms,
         sample_interval_ms,
         last_binary_sample_ms,
         baseline,
         peak,
         operations
       ) do
    timeout_ms = min(remaining_timeout!(pid, deadline_ms), sample_interval_ms)

    receive do
      {:measurement_phase_finish, ^pid, ^name, duration_us} ->
        final = process_sample(pid, true)
        peak = maximum_sample(peak, final)
        send(pid, {:measurement_phase_recorded, name})

        operation = %{
          duration_us: duration_us,
          baseline_process_memory_bytes: baseline.process_memory_bytes,
          peak_process_memory_bytes: peak.process_memory_bytes,
          process_memory_delta_bytes:
            max(peak.process_memory_bytes - baseline.process_memory_bytes, 0),
          baseline_referenced_binary_bytes: baseline.referenced_binary_bytes,
          peak_referenced_binary_bytes: peak.referenced_binary_bytes,
          referenced_binary_delta_bytes:
            max(peak.referenced_binary_bytes - baseline.referenced_binary_bytes, 0)
        }

        {operation, operations}

      {:measurement_failed, ^pid, formatted_error} ->
        Process.demonitor(monitor_ref, [:flush])
        raise "manifest scalability measurement failed:\n#{formatted_error}"

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        raise "manifest scalability measurement process exited during #{name}: #{inspect(reason)}"
    after
      timeout_ms ->
        now_ms = System.monotonic_time(:millisecond)
        include_binaries? = now_ms - last_binary_sample_ms >= @binary_sample_interval_ms
        sample = process_sample(pid, include_binaries?)

        last_binary_sample_ms =
          if include_binaries?, do: now_ms, else: last_binary_sample_ms

        collect_phase(
          pid,
          monitor_ref,
          name,
          deadline_ms,
          sample_interval_ms,
          last_binary_sample_ms,
          baseline,
          maximum_sample(peak, sample),
          operations
        )
    end
  end

  defp sizes(encoded, gzip, decoded, asset_count) do
    canonical_bytes = byte_size(encoded)
    gzip_bytes = byte_size(gzip)
    word_size = :erlang.system_info(:wordsize)

    %{
      canonical_json_bytes: canonical_bytes,
      gzip_bytes: gzip_bytes,
      gzip_ratio: Float.round(gzip_bytes / canonical_bytes, 6),
      canonical_bytes_per_asset: div(canonical_bytes, asset_count),
      gzip_bytes_per_asset: div(gzip_bytes, asset_count),
      decoded_flat_heap_bytes: :erts_debug.flat_size(decoded) * word_size,
      decoded_external_term_bytes: :erlang.external_size(decoded)
    }
  end

  defp attribution(decoded) do
    assets = Map.fetch!(decoded, "assets")
    assurances = assets |> Enum.map(&Map.get(&1, "assurance")) |> Enum.reject(&is_nil/1)
    checks = Enum.flat_map(assurances, &Map.fetch!(&1, "checks"))

    %{
      graph_json_bytes: encoded_size(Map.fetch!(decoded, "graph")),
      metadata_json_bytes: encoded_size(Map.fetch!(decoded, "metadata")),
      asset_field_value_json_bytes: aggregate_field_sizes(assets),
      assurance_field_value_json_bytes: aggregate_field_sizes(assurances),
      check_field_value_json_bytes: aggregate_field_sizes(checks)
    }
  end

  defp aggregate_field_sizes(values) do
    Enum.reduce(values, %{}, fn value, totals ->
      Enum.reduce(value, totals, fn {key, field_value}, field_totals ->
        field_size = encoded_size(field_value)
        Map.update(field_totals, key, field_size, &(&1 + field_size))
      end)
    end)
  end

  defp encoded_size(value) do
    value
    |> Jason.encode_to_iodata!()
    |> :erlang.iolist_size()
  end

  defp process_sample(pid), do: process_sample(pid, true)

  defp process_sample(pid, true) do
    case Process.info(pid, [:memory, :binary]) do
      [memory: memory, binary: binaries] ->
        referenced_binary_bytes =
          binaries
          |> Enum.uniq_by(&elem(&1, 0))
          |> Enum.sum_by(&elem(&1, 1))

        %{
          process_memory_bytes: memory,
          referenced_binary_bytes: referenced_binary_bytes
        }

      nil ->
        %{process_memory_bytes: 0, referenced_binary_bytes: 0}
    end
  end

  defp process_sample(pid, false) do
    case Process.info(pid, :memory) do
      {:memory, memory} ->
        %{process_memory_bytes: memory, referenced_binary_bytes: 0}

      nil ->
        %{process_memory_bytes: 0, referenced_binary_bytes: 0}
    end
  end

  defp maximum_sample(left, right) do
    %{
      process_memory_bytes: max(left.process_memory_bytes, right.process_memory_bytes),
      referenced_binary_bytes: max(left.referenced_binary_bytes, right.referenced_binary_bytes)
    }
  end

  defp environment do
    %{
      elixir: System.version(),
      otp_release: :erlang.system_info(:otp_release) |> List.to_string(),
      architecture: :erlang.system_info(:system_architecture) |> List.to_string(),
      word_size_bytes: :erlang.system_info(:wordsize),
      schedulers_online: :erlang.system_info(:schedulers_online)
    }
  end

  defp validate_options!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "manifest scalability measurement options must be a keyword list"
    end

    case Enum.find(Keyword.keys(opts), &(&1 not in @allowed_opts)) do
      nil ->
        :ok

      key ->
        raise ArgumentError, "unknown manifest scalability measurement option #{inspect(key)}"
    end

    sql_columns =
      Keyword.get(opts, :sql_columns, ManifestScalabilityFixture.default_sql_columns())

    contract_columns =
      Keyword.get(
        opts,
        :contract_columns,
        ManifestScalabilityFixture.default_contract_columns()
      )

    # Let the fixture enforce its domain-specific bounds before spawning a worker.
    ManifestScalabilityFixture.build(1,
      sql_columns: sql_columns,
      contract_columns: contract_columns
    )

    %{
      sql_columns: sql_columns,
      contract_columns: contract_columns,
      sample_interval_ms:
        positive_integer!(opts, :sample_interval_ms, @default_sample_interval_ms),
      timeout_ms: positive_integer!(opts, :timeout_ms, @default_timeout_ms),
      progress: boolean!(opts, :progress, false)
    }
  end

  defp positive_integer!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
    end
  end

  defp boolean!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> value
      value -> raise ArgumentError, "#{key} must be a boolean, got: #{inspect(value)}"
    end
  end

  defp remaining_timeout!(pid, deadline_ms) do
    case deadline_ms - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> remaining
      _expired -> timeout!(pid)
    end
  end

  defp timeout!(pid) do
    Process.exit(pid, :kill)
    raise "manifest scalability measurement timed out"
  end
end
