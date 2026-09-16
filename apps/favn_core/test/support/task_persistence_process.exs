Code.require_file("../../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)
alias Favn.Contracts.RunnerTask.PersistenceCodec, as: Codec
alias Favn.Manifest.Serializer
alias Favn.Manifest.Version
alias FavnTestSupport.RunnerTaskPersistence, as: Fixture
[mode, directory] = System.argv()
file = Path.join(directory, "tasks.json")

case mode do
  "write" ->
    module = "Elixir.ConsumerFresh#{System.unique_integer([:positive])}"
    name = "fresh_asset_#{System.unique_integer([:positive])}"
    resolver = module <> "Resolver"
    {version, package} = Fixture.package_version(module, name, resolver)
    application_key = "fresh_application_key_#{System.unique_integer([:positive])}"
    application_value = "fresh_application_value_#{System.unique_integer([:positive])}"
    adapter_key = "fresh_adapter_key_#{System.unique_integer([:positive])}"
    application_key_atom = String.to_atom(application_key)
    application_value_atom = String.to_atom(application_value)
    adapter_key_atom = String.to_atom(adapter_key)

    tasks =
      Enum.map(Fixture.tasks(version), fn {kind, payload, result} ->
        payload =
          if kind == :asset_attempt, do: %{payload | execution_package: package}, else: payload

        result =
          case {kind, result} do
            {:asset_attempt, %{asset_results: [asset_result]}} ->
              %{
                result
                | asset_results: [
                    %{
                      asset_result
                      | meta: %{application_key_atom => application_value_atom}
                    }
                  ]
              }

            {:relation_inspection, inspection} ->
              %{
                inspection
                | table_metadata: %{adapter_key_atom => application_value_atom}
              }

            _other ->
              result
          end

        {:ok, encoded, hash} = Codec.encode_payload(kind, payload)
        {:ok, encoded_result} = Codec.encode_result(kind, :succeeded, result)
        packages = if kind == :asset_attempt, do: [package], else: []

        {:ok, persisted_result} =
          Codec.decode_result(kind, :succeeded, encoded_result, version, packages)

        %{
          kind: Atom.to_string(kind),
          payload: encoded,
          result: encoded_result,
          hash: Base.encode16(hash),
          payload_digest: Fixture.digest(payload),
          result_digest: Fixture.digest(persisted_result)
        }
      end)

    failed_result = Fixture.failed_result(version)
    {:ok, failed_encoded} = Codec.encode_result(:asset_attempt, :failed, failed_result)

    {:ok, persisted_failure} =
      Codec.decode_result(:asset_attempt, :failed, failed_encoded, version, [package])

    File.write!(
      file,
      Jason.encode!(%{
        module: module,
        name: name,
        resolver: resolver,
        fresh_names: [application_key, application_value, adapter_key],
        failure: failed_encoded,
        failure_digest: Fixture.digest(persisted_failure),
        validations:
          Enum.map(Fixture.validations(), fn value ->
            {:ok, encoded} = Favn.Contracts.RunnerTask.PersistenceData.encode(value, 262_144)
            %{encoded: encoded, digest: Fixture.digest(value)}
          end),
        package: Serializer.encode_manifest!(package),
        manifest: Serializer.encode_manifest!(version.manifest),
        id: version.manifest_version_id,
        content_hash: version.content_hash,
        releases: version.runner_releases,
        tasks: tasks
      })
    )

  "read" ->
    data = file |> File.read!() |> Jason.decode!()

    for name <- [data["module"], data["name"], data["resolver"]] ++ data["fresh_names"] do
      try do
        String.to_existing_atom(name)
        raise "consumer atom unexpectedly present"
      rescue
        ArgumentError -> :ok
      end
    end

    {:ok, manifest} = Serializer.decode_manifest(data["manifest"])

    {:ok, version} =
      Version.from_published(manifest,
        manifest_version_id: data["id"],
        content_hash: data["content_hash"],
        runner_releases: data["releases"]
      )

    {:ok, package} = Favn.Manifest.ExecutionPackage.from_published(Jason.decode!(data["package"]))

    for value <- data["validations"] do
      {:ok, decoded} = Favn.Contracts.RunnerTask.PersistenceData.decode(value["encoded"], 262_144)
      true = Fixture.digest(decoded) == value["digest"]
    end

    {:ok, failure} =
      Codec.decode_result(:asset_attempt, :failed, data["failure"], version, [package])

    true = Fixture.digest(failure) == data["failure_digest"]
    kinds = Map.new(Favn.Contracts.RunnerTask.task_kinds(), &{Atom.to_string(&1), &1})

    for task <- data["tasks"] do
      kind = Map.fetch!(kinds, task["kind"])
      packages = if kind == :asset_attempt, do: [package], else: []
      {:ok, payload} = Codec.decode_payload(kind, task["payload"], version, packages)
      {:ok, result} = Codec.decode_result(kind, :succeeded, task["result"], version, packages)
      {:ok, hash} = Codec.payload_hash(task["payload"])
      true = Fixture.digest(payload) == task["payload_digest"]
      true = Fixture.digest(result) == task["result_digest"]
      true = Base.encode16(hash) == task["hash"]
    end
end

IO.puts("#{mode}: 8 exact task round trips")
