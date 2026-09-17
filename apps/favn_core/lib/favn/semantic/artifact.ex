defmodule Favn.Semantic.Artifact do
  @moduledoc """
  Immutable, source-free analytical catalog with a closed JSON wire contract.

  Nested records use string keys and finite enums. Decoding never creates atoms,
  resolves modules or evaluates formulas. Digests detect accidental corruption;
  they are content identities, not signatures authenticating an untrusted author.
  Consumers must obtain artifacts from a trusted build or publisher.
  """

  alias Favn.Manifest.Serializer
  alias Favn.Semantic.{Diagnostic, Schema, Snapshot}

  @max_bytes 16 * 1024 * 1024
  @enforce_keys [:semantic_version, :snapshot_version, :snapshot, :models, :compiler]
  defstruct [:semantic_version, :snapshot_version, :snapshot, :models, :compiler]

  @type t :: %__MODULE__{
          semantic_version: String.t(),
          snapshot_version: String.t(),
          snapshot: [map()],
          models: [map()],
          compiler: map()
        }

  @doc "Creates a canonical version from compiled, closed model and snapshot records."
  @spec new([map()], [map()]) :: {:ok, t()} | {:error, [Diagnostic.t()]}
  def new(models, snapshot) do
    artifact = %__MODULE__{
      semantic_version: "",
      snapshot_version: Snapshot.digest("dc_", snapshot),
      snapshot: snapshot,
      models: models,
      compiler: %{"name" => "favn-semantic", "version" => 1, "dialect" => "duckdb"}
    }

    artifact = %{artifact | semantic_version: Snapshot.digest("sm_", payload(artifact))}

    case encode(artifact) do
      {:ok, _json} ->
        {:ok, artifact}

      {:error, reason} ->
        {:error,
         [
           Diagnostic.new(
             reason,
             "The compiled semantic artifact is invalid or exceeds its size limit."
           )
         ]}
    end
  end

  @doc "Returns canonical JSON after validating structure, references and identities."
  @spec encode(t()) :: {:ok, String.t()} | {:error, atom()}
  def encode(%__MODULE__{} = artifact) do
    wire = Map.put(payload(artifact), "semantic_version", artifact.semantic_version)

    with :ok <- Schema.validate(wire),
         :ok <- identities(wire),
         json <- Serializer.encode_canonical!(wire),
         true <- byte_size(json) <= @max_bytes do
      {:ok, json}
    else
      false -> {:error, :artifact_too_large}
      {:error, _} = error -> error
    end
  end

  @doc "Decodes bounded JSON without atom creation or source-code loading."
  @spec decode(binary()) :: {:ok, t()} | {:error, atom()}
  def decode(json) when is_binary(json) and byte_size(json) <= @max_bytes do
    with {:ok, wire} <- Jason.decode(json),
         :ok <- Schema.validate(wire),
         :ok <- identities(wire) do
      {:ok,
       %__MODULE__{
         semantic_version: wire["semantic_version"],
         snapshot_version: wire["snapshot_version"],
         snapshot: wire["snapshot"],
         models: wire["models"],
         compiler: wire["compiler"]
       }}
    else
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _ -> {:error, :invalid_json}
    end
  end

  def decode(_), do: {:error, :artifact_too_large}

  @doc "Reads a finished local artifact with a size check before allocation."
  @spec read(Path.t()) :: {:ok, t()} | {:error, term()}
  def read(path) do
    with {:ok, json} <- bounded_read(path) do
      decode(json)
    end
  end

  @doc "Atomically writes `<directory>/<semantic_version>/semantic.json`, without replacement."
  @spec write(t(), Path.t()) ::
          {:ok, %{path: Path.t(), semantic_version: String.t()}} | {:error, term()}
  def write(%__MODULE__{} = artifact, directory) do
    with {:ok, json} <- encode(artifact), :ok <- File.mkdir_p(directory) do
      destination = Path.join(directory, artifact.semantic_version)
      path = Path.join(destination, "semantic.json")

      case bounded_read(path) do
        {:ok, ^json} -> {:ok, %{path: path, semantic_version: artifact.semantic_version}}
        {:ok, _} -> {:error, :immutable_artifact_conflict}
        {:error, :enoent} -> write_new(artifact, json, directory, destination, path)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp write_new(artifact, json, directory, destination, path) do
    temporary =
      Path.join(
        directory,
        ".semantic-" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
      )

    with :ok <- File.mkdir(temporary) do
      result =
        with :ok <- File.write(Path.join(temporary, "semantic.json"), json, [:binary, :exclusive]),
             :ok <- File.rename(temporary, destination),
             do: {:ok, %{path: path, semantic_version: artifact.semantic_version}}

      File.rm_rf(temporary)

      case result do
        {:error, reason} when reason in [:eexist, :enotempty] ->
          case bounded_read(path) do
            {:ok, ^json} -> {:ok, %{path: path, semantic_version: artifact.semantic_version}}
            _ -> {:error, :immutable_artifact_conflict}
          end

        result ->
          result
      end
    end
  end

  defp bounded_read(path) do
    case File.open(path, [:read, :binary]) do
      {:ok, io} ->
        try do
          case IO.binread(io, @max_bytes + 1) do
            data when is_binary(data) and byte_size(data) <= @max_bytes -> {:ok, data}
            data when is_binary(data) -> {:error, :artifact_too_large}
            :eof -> {:ok, ""}
            {:error, _} = error -> error
          end
        after
          File.close(io)
        end

      error ->
        error
    end
  end

  defp payload(artifact),
    do: %{
      "schema_version" => 1,
      "snapshot_version" => artifact.snapshot_version,
      "snapshot" => artifact.snapshot,
      "models" => artifact.models,
      "compiler" => artifact.compiler
    }

  defp identities(wire) do
    cond do
      wire["snapshot_version"] != Snapshot.digest("dc_", wire["snapshot"]) ->
        {:error, :snapshot_digest_mismatch}

      wire["semantic_version"] != Snapshot.digest("sm_", Map.delete(wire, "semantic_version")) ->
        {:error, :semantic_digest_mismatch}

      true ->
        :ok
    end
  end
end
