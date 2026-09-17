defmodule Favn.Catalog.Artifact do
  @moduledoc """
  Immutable public manifest catalog, without executable packages or runtime secrets.

  The source manifest identity is retained; `mc_` identifies the public projection.
  This is a trusted-build artifact. Hashes prove integrity, not author authenticity.
  Decoding never resolves customer modules or creates atoms. Inputs are bounded to
  64 MiB and 10,000 assets. The public wire schema is versioned independently.
  """
  alias Favn.Catalog.{Schema, Value}
  alias Favn.Manifest.{Publication, Serializer}
  alias Favn.Semantic.Snapshot

  @max_bytes 64 * 1024 * 1024
  @enforce_keys [:document]
  defstruct [:document]
  @type t :: %__MODULE__{document: map()}

  @doc "Projects the exact verified publication into public, source-free definitions."
  @spec new(Publication.t()) :: {:ok, t()} | {:error, term()}
  def new(%Publication{} = input) do
    with {:ok, publication} <- Publication.from_parts(input.version, input.execution_packages) do
      manifest = publication.version.manifest
      packages = Publication.packages_by_hash(publication)

      assets =
        Enum.map(manifest.assets, fn asset ->
          package = packages[asset.execution_package_hash]
          Map.put(asset, :contract, if(package, do: package.sql_execution.contract))
        end)

      with {:ok, snapshot} <- Snapshot.build(assets) do
        details = Map.new(assets, &{Snapshot.ref(&1.ref), &1})

        document = %{
          "schema_version" => 1,
          "manifest_version" => publication.version.manifest_version_id,
          "manifest_hash" => publication.version.content_hash,
          "assets" =>
            Enum.map(snapshot, fn record ->
              asset = details[record["ref"]]

              Map.merge(record, %{
                "description" => asset.description,
                "runtime_requirements" => runtime_requirements(asset.runtime_config),
                "checks" =>
                  Enum.map(
                    (packages[asset.execution_package_hash] &&
                       packages[asset.execution_package_hash].sql_execution.checks) || [],
                    fn check ->
                      Value.encode(
                        Map.take(check, [
                          :name,
                          :at,
                          :on_violation,
                          :when,
                          :message,
                          :origin,
                          :claim_id
                        ])
                      )
                    end
                  ),
                "category" => asset.metadata[:category],
                "tags" => Map.get(asset.metadata, :tags, []),
                "policies" =>
                  Value.encode(
                    Map.take(asset, [
                      :window,
                      :coverage,
                      :freshness,
                      :retry_policy,
                      :materialization,
                      :partition_spec,
                      :execution_pool,
                      :runner_pool,
                      :settings
                    ])
                  )
              })
            end),
          "pipelines" => manifest.pipelines |> Enum.map(&pipeline/1) |> Enum.sort_by(& &1["ref"]),
          "schedules" => manifest.schedules |> Enum.map(&schedule/1) |> Enum.sort_by(& &1["ref"]),
          "policies" =>
            Value.encode(
              Map.take(manifest, [
                :environment,
                :execution_pools,
                :connection_circuits,
                :runner_releases
              ])
            )
        }

        artifact = %__MODULE__{
          document: Map.put(document, "catalog_version", Snapshot.digest("mc_", document))
        }

        with {:ok, _} <- encode(artifact), do: {:ok, artifact}
      end
    end
  end

  @doc "Validates and canonically encodes a public manifest catalog."
  @spec encode(t()) :: {:ok, binary()} | {:error, atom()}
  def encode(%__MODULE__{document: document}) do
    with :ok <- Schema.validate(document),
         true <-
           document["catalog_version"] ==
             Snapshot.digest("mc_", Map.delete(document, "catalog_version")),
         json <- Serializer.encode_canonical!(document),
         true <- byte_size(json) <= @max_bytes do
      {:ok, json}
    else
      false -> {:error, :invalid_catalog_identity_or_size}
      error -> error
    end
  rescue
    _ in [ArgumentError, Protocol.UndefinedError] -> {:error, :invalid_catalog_artifact}
  end

  @doc "Decodes bounded closed JSON, validating references and immutable identity."
  @spec decode(binary()) :: {:ok, t()} | {:error, atom()}
  def decode(json) when is_binary(json) and byte_size(json) <= @max_bytes do
    with {:ok, document} <- Jason.decode(json),
         artifact = %__MODULE__{document: document},
         {:ok, _} <- encode(artifact) do
      {:ok, artifact}
    else
      _ -> {:error, :invalid_catalog_artifact}
    end
  end

  def decode(_), do: {:error, :artifact_too_large}

  @doc "Reads at most the artifact limit plus one byte before decoding."
  @spec read(Path.t()) :: {:ok, t()} | {:error, term()}
  def read(path) do
    with {:ok, io} <- File.open(path, [:read, :binary]) do
      try do
        case IO.binread(io, @max_bytes + 1) do
          data when is_binary(data) -> decode(data)
          _ -> {:error, :invalid_catalog_artifact}
        end
      after
        File.close(io)
      end
    end
  end

  @doc "Atomically writes an immutable catalog directory, verifying existing bytes."
  @spec write(t(), Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def write(%__MODULE__{document: document} = artifact, directory) do
    with {:ok, json} <- encode(artifact), :ok <- File.mkdir_p(directory) do
      destination = Path.join(directory, document["catalog_version"])
      path = Path.join(destination, "catalog.json")

      temporary =
        Path.join(directory, ".catalog-" <> Base.encode16(:crypto.strong_rand_bytes(12)))

      with :ok <- File.mkdir(temporary) do
        try do
          with :ok <-
                 File.write(Path.join(temporary, "catalog.json"), json, [:binary, :exclusive]) do
            case File.rename(temporary, destination) do
              :ok ->
                {:ok, path}

              {:error, reason} when reason in [:eexist, :enotempty] ->
                case read(path) do
                  {:ok, ^artifact} -> {:ok, path}
                  _ -> {:error, :immutable_artifact_conflict}
                end

              error ->
                error
            end
          end
        after
          File.rm_rf(temporary)
        end
      end
    end
  end

  defp pipeline(value) do
    %{
      "ref" => Snapshot.ref({value.module, value.name}),
      "selectors" => Enum.map(value.selectors, &selector/1),
      "deps" => to_string(value.deps),
      "schedule" =>
        case value.schedule do
          nil -> nil
          {:inline, item} -> %{"inline" => schedule(item)}
          {:ref, ref} -> %{"ref" => Snapshot.ref(ref)}
        end,
      "policies" =>
        Value.encode(
          Map.take(value, [
            :window,
            :retry_policy,
            :max_concurrency,
            :execution_pool,
            :runner_pool,
            :resource_recovery,
            :source,
            :outputs,
            :settings
          ])
        ),
      "category" => value.metadata[:category],
      "tags" => Map.get(value.metadata, :tags, [])
    }
  end

  defp selector({:asset, ref}), do: ["asset", Snapshot.ref(ref)]
  defp selector({:module, module}), do: ["module", Enum.join(Module.split(module), ".")]

  defp selector({kind, value}) when kind in [:tag, :category],
    do: [to_string(kind), to_string(value)]

  defp schedule(value) do
    value
    |> Map.take([:kind, :cron, :timezone, :timezone_source, :missed, :overlap, :origin])
    |> Value.encode()
    |> Map.put("ref", Snapshot.ref({value.module, value.name}))
  end

  defp runtime_requirements(declarations) do
    for {scope, fields} <- declarations,
        {field, ref} <- fields,
        not ref.secret? do
      %{
        "scope" => to_string(scope),
        "field" => to_string(field),
        "provider" => to_string(ref.provider),
        "key" => ref.key,
        "required" => ref.required?
      }
    end
    |> Enum.sort_by(&{&1["scope"], &1["field"]})
  end
end
