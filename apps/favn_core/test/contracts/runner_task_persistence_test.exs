Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule Favn.Contracts.RunnerTaskPersistenceTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
  alias Favn.Contracts.RunnerTask.PersistenceData, as: Data
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

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

    {other, wrong} =
      Fixture.package_version("Elixir.WrongPackage", "other", "Elixir.OtherResolver")

    assert {:error, _} = Codec.decode_payload(:asset_attempt, encoded, version, [wrong])
    assert {:error, _} = Codec.decode_payload(:asset_attempt, encoded, other, [package])

    assert {:error, _} =
             Codec.decode_payload(:asset_attempt, encoded, version, [
               %{package | sql_execution: %{package.sql_execution | sql: "SELECT 2"}}
             ])

    for replacement <- ["not-a-hash", String.duplicate("A", 64), String.duplicate("a", 65)] do
      changed =
        update_package_hash(encoded, fn _ ->
          [["atom", "content_hash"], ["binary", Base.encode64(replacement)]]
        end)

      assert {:error, _} = Codec.package_hash(changed)
    end

    duplicate =
      update_in(encoded, ["payload", "data"], fn ["struct", name, ["map", pairs]] ->
        ["struct", name, ["map", pairs ++ [hd(pairs)]]]
      end)

    assert {:error, _} = Codec.package_hash(duplicate)

    duplicate_hash =
      update_in(encoded, ["payload", "data"], fn ["struct", name, ["map", pairs]] ->
        pairs =
          Enum.map(pairs, fn
            [["atom", "execution_package"], ["struct", package_name, ["map", fields]]] ->
              hash = Enum.find(fields, &match?([["atom", "content_hash"], _], &1))
              [["atom", "execution_package"], ["struct", package_name, ["map", fields ++ [hash]]]]

            pair ->
              pair
          end)

        ["struct", name, ["map", pairs]]
      end)

    assert {:error, _} = Codec.package_hash(duplicate_hash)
  end

  defp update_package_hash(envelope, change) do
    update_in(envelope, ["payload", "data"], fn ["struct", name, ["map", pairs]] ->
      pairs =
        Enum.map(pairs, fn
          [["atom", "execution_package"], ["struct", package_name, ["map", fields]]] ->
            fields =
              Enum.map(fields, fn
                [["atom", "content_hash"], _] = field -> change.(field)
                field -> field
              end)

            [["atom", "execution_package"], ["struct", package_name, ["map", fields]]]

          pair ->
            pair
        end)

      ["struct", name, ["map", pairs]]
    end)
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
