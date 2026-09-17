Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule Favn.Contracts.RunnerTaskPersistenceTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts.RunnerTask.OpenData
  alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
  alias Favn.Contracts.RunnerTask.PersistenceData, as: Data
  alias Favn.Contracts.RunnerError
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture
  alias Favn.Manifest.Schedule
  alias Favn.Window.{Anchor, Policy, Selection}

  test "all task kinds preserve exact supported values and hash identity" do
    version = Fixture.version()

    for {kind, payload, result} <- Fixture.tasks(version) do
      assert {:ok, encoded, hash} = Codec.encode_payload(kind, payload), inspect(kind)
      assert {:ok, ^payload} = Codec.decode_payload(kind, encoded, version), inspect(kind)
      assert {:ok, ^hash} = Codec.payload_hash(encoded)
      assert {:ok, encoded_result} = Codec.encode_result(kind, :succeeded, result), inspect(kind)

      assert {:ok, ^result} = Codec.decode_result(kind, :succeeded, encoded_result, version),
             inspect(kind)
    end
  end

  test "backfill metadata keys round trip independently without retained artifacts" do
    for key <- [
          :backfill_id,
          :backfill_window_id,
          :backfill_window_key,
          :backfill_execution_group_id,
          :backfill_root_run_id,
          :operator_metadata
        ] do
      value = %{key => "example"}
      assert {:ok, encoded} = Data.encode(value, 1_048_576)
      assert {:ok, ^value} = Data.decode(encoded, 1_048_576), inspect(key)
    end
  end

  test "an existing unknown metadata atom is still rejected" do
    assert {:ok, encoded} = Data.encode(%{unregistered_runner_metadata: "example"}, 1_048_576)
    assert {:error, :invalid_runner_task_data} = Data.decode(encoded, 1_048_576)
  end

  test "application result metadata normalizes without framework atom registration" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset_result] = result.asset_results

    metadata = %{
      manifest_uri: "az://landing/manifest.json",
      landing_run_id: "landing-1",
      favn_run_id: "run-1",
      pages_written: 3,
      load_mode: :append,
      nested: %{application_key: ~U[2026-09-16 09:00:00Z]}
    }

    attempt = %{
      attempt: 1,
      started_at: ~U[2026-09-16 09:00:00Z],
      finished_at: ~U[2026-09-16 09:00:01Z],
      duration_ms: 1_000,
      status: :ok,
      meta: metadata,
      error: nil
    }

    result = %{result | asset_results: [%{asset_result | meta: metadata, attempts: [attempt]}]}

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :succeeded, result)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :succeeded, encoded, version)
    [persisted_asset] = persisted.asset_results

    expected = %{
      "manifest_uri" => "az://landing/manifest.json",
      "landing_run_id" => "landing-1",
      "favn_run_id" => "run-1",
      "pages_written" => 3,
      "load_mode" => "append",
      "nested" => %{"application_key" => ~U[2026-09-16 09:00:00Z]}
    }

    assert persisted_asset.meta == expected
    assert hd(persisted_asset.attempts).meta == expected
  end

  test "typed SQL result fields survive while runtime input metadata normalizes" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset_result] = result.asset_results

    check =
      Favn.SQL.CheckResult.new(name: :row_count, phase: :after_materialize, outcome: :passed)

    {_kind, _request, inspection} =
      Enum.find(Fixture.tasks(version), &(elem(&1, 0) == :relation_inspection))

    metadata = %{
      materialized: inspection.relation_ref,
      connection: :default,
      rows_affected: 1,
      command: "INSERT",
      check_results: [check],
      quality_status: :passed,
      write_outcome: :written,
      reason: nil,
      group_replacement: nil,
      runtime_inputs: %{
        resolver: hd(version.manifest.assets).module,
        input_identity: "snapshot-1",
        input_metadata: %{source_snapshot: :ready},
        duration_ms: 4
      }
    }

    result = %{
      result
      | asset_results: [
          %{asset_result | evidence: Favn.Contracts.RunnerAssetEvidence.new!(:sql, metadata)}
        ]
    }

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :succeeded, result)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :succeeded, encoded, version)
    [persisted_asset] = persisted.asset_results

    assert persisted_asset.evidence.materialized == metadata.materialized
    assert persisted_asset.evidence.check_results == [check]
    assert persisted_asset.evidence.quality_status == :passed

    assert persisted_asset.evidence.runtime_inputs.input_metadata == %{
             "source_snapshot" => "ready"
           }

    assert_raise KeyError, fn ->
      Favn.Contracts.RunnerAssetEvidence.new!(:sql, Map.put(metadata, "command", "UPDATE"))
    end
  end

  test "typed SQL failure controls survive even without check results" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset_result] = result.asset_results

    metadata = %{
      connection: :default,
      check_results: [],
      quality_status: :failed,
      transaction_outcome: :not_started,
      write_outcome: :not_started
    }

    result = %{
      result
      | status: :error,
        asset_results: [
          %{
            asset_result
            | status: :error,
              evidence: Favn.Contracts.RunnerAssetEvidence.new!(:sql, metadata)
          }
        ]
    }

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :failed, result)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :failed, encoded, version)

    assert hd(persisted.asset_results).evidence ==
             Favn.Contracts.RunnerAssetEvidence.new!(:sql, metadata)
  end

  test "application metadata remains open when a key overlaps the SQL envelope" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset_result] = result.asset_results

    metadata = %{check_results: :application_value, reason: :application_reason}
    result = %{result | asset_results: [%{asset_result | meta: metadata}]}

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :succeeded, result)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :succeeded, encoded, version)

    assert hd(persisted.asset_results).meta == %{
             "check_results" => "application_value",
             "reason" => "application_reason"
           }
  end

  test "application control-like keys never become execution evidence" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset] = result.asset_results

    meta = %{
      observed: true,
      relation: "app-relation",
      status: :error,
      write_outcome: :written,
      quality_status: :passed,
      retryable?: true
    }

    result = %{result | asset_results: [%{asset | meta: meta, evidence: nil}]}

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :succeeded, result)
    assert {:ok, decoded} = Codec.decode_result(:asset_attempt, :succeeded, encoded, version)
    assert [%{status: :ok, evidence: nil, meta: normalized}] = decoded.asset_results

    assert normalized ==
             Map.new(meta, fn {key, value} ->
               {Atom.to_string(key),
                if(is_atom(value) and not is_boolean(value),
                  do: Atom.to_string(value),
                  else: value
                )}
             end)
  end

  test "malformed execution evidence is rejected on encode and decode" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset] = result.asset_results
    invalid = %{result | asset_results: [%{asset | evidence: %{asset.evidence | kind: :ok}}]}

    assert {:error, :invalid_runner_asset_evidence} =
             Codec.encode_result(:asset_attempt, :succeeded, invalid)

    assert {:ok, envelope} = Codec.encode_result(:asset_attempt, :succeeded, result)
    assert {:ok, bad_data} = Data.encode(invalid, 1_048_576)

    assert {:error, _} =
             Codec.decode_result(
               :asset_attempt,
               :succeeded,
               Map.put(envelope, "payload", bad_data),
               version
             )
  end

  test "operator metadata is bounded open data inside an explicit work field" do
    version = Fixture.version()
    {:asset_attempt, work, _result} = hd(Fixture.tasks(version))

    work = %{
      work
      | metadata: %{operator_metadata: %{new_application_key: :new_application_value}}
    }

    assert {:ok, encoded, _hash} = Codec.encode_payload(:asset_attempt, work)
    assert {:ok, decoded} = Codec.decode_payload(:asset_attempt, encoded, version)

    assert decoded.metadata.operator_metadata == %{
             "new_application_key" => "new_application_value"
           }

    work = %{work | metadata: %{operator_metadata: %{"id" => 2, id: 1}}}

    assert {:error, {:invalid_runner_task_open_data, :operator_metadata, :duplicate_key}} =
             Codec.encode_payload(:asset_attempt, work)
  end

  test "runner error and inspection adapter extensions normalize without weakening controls" do
    version = Fixture.version()
    failed = Fixture.failed_result(version)

    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :failed, failed)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :failed, encoded, version)

    assert hd(persisted.asset_results).error.details["contract_validation"]["status"] ==
             "failed"

    error =
      RunnerError.new(
        type: :landing_failed,
        details: %{adapter_detail: :temporary, nested: %{attempt_code: 4}},
        outcome: :safe_failure
      )

    failed = %{failed | error: error}
    assert {:ok, encoded} = Codec.encode_result(:asset_attempt, :failed, failed)
    assert {:ok, persisted} = Codec.decode_result(:asset_attempt, :failed, encoded, version)

    assert persisted.error.details == %{
             "adapter_detail" => "temporary",
             "nested" => %{"attempt_code" => 4}
           }

    {:relation_inspection, _request, inspection} =
      Enum.find(Fixture.tasks(version), &(elem(&1, 0) == :relation_inspection))

    [column] = inspection.columns

    inspection = %{
      inspection
      | relation: %{inspection.relation | metadata: %{adapter_extension: :present}},
        columns: [
          %{column | metadata: %{contract_nullability: :reliable, adapter_extension: :present}}
        ],
        sample: %{limit: 1, columns: ["id"], rows: [%{adapter_value: :present}]},
        table_metadata: %{relation_instance_id: "instance-1", adapter_extension: :present},
        error: %{adapter_code: :none}
    }

    assert {:ok, encoded} = Codec.encode_result(:relation_inspection, :succeeded, inspection)

    assert {:ok, persisted} =
             Codec.decode_result(:relation_inspection, :succeeded, encoded, version)

    assert persisted.relation.metadata == %{"adapter_extension" => "present"}

    assert hd(persisted.columns).metadata == %{
             :contract_nullability => :reliable,
             "adapter_extension" => "present"
           }

    assert persisted.sample.rows == [%{"adapter_value" => "present"}]

    assert persisted.table_metadata == %{
             "relation_instance_id" => "instance-1",
             "adapter_extension" => "present"
           }

    assert persisted.error == %{"adapter_code" => "none"}

    duplicate_nullability = %{
      inspection
      | columns: [
          %{
            column
            | metadata: %{"contract_nullability" => :reliable, contract_nullability: nil}
          }
        ]
    }

    assert {:error, {:invalid_runner_task_open_data, :inspection_column_metadata, :duplicate_key}} =
             Codec.encode_result(
               :relation_inspection,
               :succeeded,
               duplicate_nullability
             )
  end

  test "application metadata rejects normalized key collisions and unsupported structs" do
    version = Fixture.version()
    {:asset_attempt, _work, result} = hd(Fixture.tasks(version))
    [asset_result] = result.asset_results

    collision = %{asset_result | meta: %{"landing_run_id" => "two", landing_run_id: "one"}}
    result = %{result | asset_results: [collision]}

    assert {:error, {:invalid_runner_task_open_data, :asset_metadata, :duplicate_key}} =
             Codec.encode_result(:asset_attempt, :succeeded, result)

    unsupported = %{asset_result | meta: %{landing_target: %URI{scheme: "https"}}}
    result = %{result | asset_results: [unsupported]}

    assert {:error, {:invalid_runner_task_open_data, :asset_metadata, :unsupported_value}} =
             Codec.encode_result(:asset_attempt, :succeeded, result)

    tuple = %{asset_result | meta: %{partition: {:year, 2026}}}
    result = %{result | asset_results: [tuple]}

    assert {:error, {:invalid_runner_task_open_data, :asset_metadata, :unsupported_value}} =
             Codec.encode_result(:asset_attempt, :succeeded, result)
  end

  test "open result data enforces depth, node and encoded-byte bounds" do
    assert {:error, :too_deep} =
             OpenData.normalize(%{"outer" => %{"inner" => %{"value" => 1}}}, max_depth: 1)

    assert {:error, :too_many_values} =
             OpenData.normalize(%{"one" => 1, "two" => 2}, max_nodes: 2)

    assert {:error, :too_large} =
             OpenData.normalize(%{"payload" => String.duplicate("x", 128)}, max_bytes: 64)
  end

  test "framework retry, rebuild, recovery and draining metadata survive complete work round trips" do
    version = Fixture.version()
    {:asset_attempt, work, _result} = hd(Fixture.tasks(version))

    cases = [
      {%{kind: :rerun},
       %{
         runtime_input_expectation: %{
           resolver: "example",
           input_identity: "input",
           payload_fingerprint: "fingerprint"
         }
       }},
      {%{
         kind: :resource_recovery,
         source_run_id: "source",
         resource_kind: :connection,
         resource_name: "warehouse"
       },
       %{
         resource_recovery_source_run_id: "source",
         resource_recovery_resource: Favn.Resource.Ref.new!(:connection, "warehouse"),
         resource_recovery_candidate_ids: ["candidate"]
       }},
      {work.trigger,
       %{
         stage_draining_after_failure: %{
           stage: 0,
           attempt: 1,
           failed_asset_ref: work.asset_ref,
           pending_task_ids: ["rt_pending"]
         }
       }}
    ]

    for {trigger, metadata} <- cases do
      value = %{work | trigger: trigger, metadata: Map.merge(work.metadata, metadata)}
      assert {:ok, encoded, _hash} = Codec.encode_payload(:asset_attempt, value)
      assert {:ok, ^value} = Codec.decode_payload(:asset_attempt, encoded, version)
    end
  end

  test "window policies round trip every supported kind, anchor and timezone source" do
    for kind <- [:hour, :day, :month, :year],
        anchor <- [:previous_complete_period, :current_period],
        source <- [nil, :local, :application_default, :utc_fallback] do
      policy =
        Policy.new!(kind,
          anchor: anchor,
          timezone: "Europe/Oslo",
          lookback: 3,
          combine_windows: true,
          allow_full_load: true
        )

      policy = %{policy | timezone_source: source}
      assert {:ok, encoded} = Data.encode(policy, 262_144)
      assert {:ok, ^policy} = Data.decode(encoded, 262_144)
    end
  end

  test "schedules round trip every supported missed, overlap, origin and timezone source" do
    for missed <- [:skip, :one, :all],
        overlap <- [:forbid, :allow, :queue_one],
        origin <- [:inline, :named],
        source <- [:local, :application_default, :utc_fallback] do
      schedule = %Schedule{
        cron: "0 6 * * *",
        timezone: "Europe/Oslo",
        timezone_source: source,
        missed: missed,
        overlap: overlap,
        origin: origin
      }

      assert {:ok, encoded} = Data.encode(schedule, 262_144)
      assert {:ok, ^schedule} = Data.decode(encoded, 262_144)
    end
  end

  test "window selections preserve scheduled, manual and backfill intents" do
    anchor = Anchor.new!(:day, ~U[2026-09-14 00:00:00Z], ~U[2026-09-15 00:00:00Z])

    for intent <- [:scheduled, :manual, :backfill] do
      expansion = if intent == :scheduled, do: {:lookback, 2}, else: :none
      assert {:ok, selection} = Selection.new(intent, [anchor], expansion, "Etc/UTC")
      assert {:ok, encoded} = Data.encode(selection, 262_144)
      assert {:ok, ^selection} = Data.decode(encoded, 262_144)
    end
  end

  test "complete pipeline context preserves nested policies without retained artifacts" do
    context = Fixture.pipeline_context()
    assert {:ok, encoded} = Data.encode(context, 262_144)
    assert {:ok, ^context} = Data.decode(encoded, 262_144)
  end

  test "scheduled pipeline triggers preserve on-time and missed occurrence metadata" do
    for recovery <- [:on_time, :missed] do
      context = put_in(Fixture.pipeline_context().trigger.occurrence.recovery, recovery)
      assert {:ok, encoded} = Data.encode(context, 262_144)
      assert {:ok, ^context} = Data.decode(encoded, 262_144)
    end
  end

  test "manual pipeline metadata preserves latest-complete window resolution" do
    context = Fixture.pipeline_context()
    assert {:ok, selection} = Selection.manual(context.anchor_window, "Europe/Oslo")

    metadata =
      Fixture.submission_metadata(context)
      |> Map.put(:window_selection, selection)
      |> Map.put(:manual_window_resolution, %{
        mode: :latest_complete,
        evaluated_at: "2026-09-15T08:00:00Z",
        availability_delay_seconds: 0
      })

    assert {:ok, encoded} = Data.encode(metadata, 262_144)
    assert {:ok, ^metadata} = Data.decode(encoded, 262_144)
  end

  test "unsupported structs remain rejected in both directions" do
    assert {:error, :invalid_runner_task_data} = Data.encode(%URI{}, 262_144)

    forged = %{
      "format" => "task-data-v1",
      "data" => ["struct", "Elixir.URI", ["map", []]]
    }

    assert {:error, :invalid_runner_task_data} = Data.decode(forged, 262_144)
  end

  test "inspection and marker results must identify the dispatched relation and target" do
    alias Favn.Contracts.RunnerTask.PersistenceSchema
    tasks = Fixture.tasks(Fixture.version())
    {kind, request, result} = Enum.find(tasks, &(elem(&1, 0) == :relation_inspection))
    assert :ok = PersistenceSchema.completion(kind, request, result, :succeeded)

    assert {:error, _} =
             PersistenceSchema.completion(
               kind,
               request,
               %{result | relation_ref: %{result.relation_ref | name: "another"}},
               :succeeded
             )

    {kind, request, result} = Enum.find(tasks, &(elem(&1, 0) == :generation_marker_read))
    assert :ok = PersistenceSchema.completion(kind, request, result, :succeeded)

    assert {:error, _} =
             PersistenceSchema.completion(
               kind,
               request,
               %{result | marker: %{result.marker | target_id: "another"}},
               :succeeded
             )
  end

  test "persisted inspection adapter text preserves the physical fingerprint" do
    {_kind, _request, inspection} =
      Enum.find(Fixture.tasks(Fixture.version()), &(elem(&1, 0) == :relation_inspection))

    assert {:ok, expected} =
             Favn.TargetCompatibility.PhysicalFingerprint.from_inspection(%{
               inspection
               | adapter: FixtureSQLAdapter
             })

    assert {:ok, ^expected} =
             Favn.TargetCompatibility.PhysicalFingerprint.from_inspection(inspection)
  end

  test "custom error labels become bounded strings without changing classification" do
    error =
      Favn.Contracts.RunnerError.new(
        type: ArgumentError,
        phase: :custom_adapter_phase,
        outcome: :safe_failure,
        retryable?: true,
        retry_after_ms: 500
      )

    expected = %{error | type: "Elixir.ArgumentError", phase: "custom_adapter_phase"}
    assert {:ok, encoded} = Data.encode(error, 262_144)
    assert {:ok, ^expected} = Data.decode(encoded, 262_144)
    assert {:ok, ^encoded} = Data.encode(expected, 262_144)
  end

  test "unknown names do not use incidental atoms and cannot create atoms" do
    known_only_in_vm = :a_test_atom_that_is_not_a_contract
    assert {:ok, envelope} = Data.encode(known_only_in_vm, 1024)
    assert {:error, :invalid_runner_task_data} = Data.decode(envelope, 1024)
    name = "untrusted_task_atom_#{System.unique_integer([:positive])}"
    forged = %{"format" => "task-data-v1", "data" => ["atom", name]}
    assert {:error, :invalid_runner_task_data} = Data.decode(forged, 1024)
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
  end

  test "duplicate keys, extra struct fields, excessive depth and wrong domain fields fail" do
    assert {:ok, encoded} = Data.encode(%{"a" => 1}, 1024)
    ["map", [pair]] = encoded["data"]
    assert {:error, _} = Data.decode(%{encoded | "data" => ["map", [pair, pair]]}, 1024)
    depth = Enum.reduce(1..70, nil, fn _, inner -> ["list", [inner]] end)
    assert {:error, _} = Data.decode(%{encoded | "data" => depth}, 1024)
    version = Fixture.version()
    {:asset_attempt, work, _result} = hd(Fixture.tasks(version))
    assert {:error, _} = Codec.encode_payload(:asset_attempt, %{work | attempt: "1"})
    assert {:error, _} = Data.encode(self(), 1024)
  end

  test "package authority is exact and malformed references cannot trigger a lookup" do
    {version, package} =
      Fixture.package_version("Elixir.PackageConsumer", "sql_asset", "Elixir.DynamicResolver")

    {:asset_attempt, work, _} = hd(Fixture.tasks(version))
    work = %{work | execution_package: package}
    assert {:ok, encoded, _} = Codec.encode_payload(:asset_attempt, work)
    assert {:ok, hash} = Codec.package_hash(encoded)
    assert hash == package.content_hash
    assert {:error, _} = Codec.decode_payload(:asset_attempt, encoded, version)
    assert {:ok, ^work} = Codec.decode_payload(:asset_attempt, encoded, version, [package])

    assert {:error, _} =
             Codec.decode_payload(
               :asset_attempt,
               %{encoded | "execution_package_hash" => nil},
               version,
               []
             )

    {other, wrong} =
      Fixture.package_version("Elixir.WrongPackage", "other", "Elixir.OtherResolver")

    assert {:error, _} = Codec.decode_payload(:asset_attempt, encoded, version, [wrong])
    assert {:error, _} = Codec.decode_payload(:asset_attempt, encoded, other, [package])

    assert {:error, _} =
             Codec.decode_payload(:asset_attempt, encoded, version, [
               %{package | sql_execution: %{package.sql_execution | sql: "SELECT 2"}}
             ])

    for replacement <- ["not-a-hash", String.duplicate("A", 64), String.duplicate("a", 65)] do
      assert {:error, _} =
               Codec.package_hash(%{encoded | "execution_package_hash" => replacement})
    end

    assert {:error, _} = Codec.package_hash(Map.put(encoded, "extra", true))

    assert {:error, _} =
             Codec.decode_payload(
               :asset_attempt,
               %{encoded | "encoding" => "task-data-v1"},
               version,
               [package]
             )

    assert {:ok, embedded} =
             Data.encode(work, Favn.Contracts.RunnerTask.Limits.payload_bytes(:asset_attempt))

    assert {:error, _} =
             Codec.decode_payload(:asset_attempt, %{encoded | "payload" => embedded}, version, [
               package
             ])

    assert {:error, _} =
             Codec.encode_payload(
               :asset_attempt,
               %{
                 work
                 | execution_package: %{
                     package
                     | sql_execution: %{package.sql_execution | sql: "SELECT 2"}
                   }
               }
             )

    assert {:ok, stripped} = Data.decode(encoded["payload"], 8_388_608, version, [], [package])
    assert stripped.execution_package == nil
    assert encoded["encoding"] == "runner-task-payload-v2"
    assert encoded["protocol_version"] == Favn.Contracts.RunnerTask.version()
    assert {:ok, ^encoded, _} = Codec.encode_payload(:asset_attempt, work)
  end

  test "compact references cannot bypass expanded work bounds" do
    alias Favn.Manifest.{ExecutionPackage, SQLExecution, Version}
    alias Favn.Contracts.RunnerTask.Limits
    version = Fixture.version()
    asset = hd(version.manifest.assets)
    sql = "SELECT 1 /*" <> String.duplicate("x", 4_200_000) <> "*/"

    {:ok, package} =
      ExecutionPackage.new(asset.ref, %SQLExecution{
        sql: sql,
        template: Favn.SQL.Template.compile!(sql, file: "large.sql", line: 1)
      })

    {:ok, version} =
      Version.new(%{
        version.manifest
        | assets: [
            %{asset | type: :sql, execution_package_hash: package.content_hash}
          ]
      })

    {:asset_attempt, stripped, _} = hd(Fixture.tasks(version))
    assert {:ok, encoded, _} = Codec.encode_payload(:asset_attempt, stripped)
    work = %{stripped | execution_package: package}

    assert {:error, {:runner_task_payload_too_large, _, _}} =
             Limits.validate_payload(:asset_attempt, work)

    assert {:error, _} = Codec.encode_payload(:asset_attempt, work)

    assert {:error, _} =
             Codec.decode_payload(
               :asset_attempt,
               %{encoded | "execution_package_hash" => package.content_hash},
               version,
               [package]
             )
  end

  test "writer and two fresh readers recover consumer atoms from retained artifacts" do
    dir = Path.join(System.tmp_dir!(), "favn-task-codec-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    script = Path.expand("../support/task_persistence_process.exs", __DIR__)
    paths = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)
    executable = System.find_executable("elixir")

    for mode <- ["write", "read", "read"] do
      {output, status} =
        System.cmd(executable, paths ++ [script, mode, dir],
          stderr_to_stdout: true,
          env: [{"ERL_FLAGS", "+S 2:2"}]
        )

      assert status == 0, output
      assert output =~ "#{mode}: 8 exact task round trips"
    end
  end
end
