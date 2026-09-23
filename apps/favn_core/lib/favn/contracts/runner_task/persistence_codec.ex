defmodule Favn.Contracts.RunnerTask.PersistenceCodec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerTask.OpenData
  alias Favn.Contracts.RunnerTask.PersistenceSchema
  alias Favn.Contracts.RunnerTask.PersistenceResult
  alias Favn.Contracts.RunnerTask.PersistenceData
  alias Favn.Contracts.RuntimeInputResolutionRequest
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  @protocol_version RunnerTask.version()
  alias Favn.Contracts.RunnerTask.Limits

  @payload_encoding "runner-task-payload-v2"

  @doc "Returns the current persisted task-payload version, independent of the wire protocol."
  @spec payload_version() :: pos_integer()
  def payload_version, do: 2

  def encode_payload(task_kind, payload) do
    with {:ok, payload} <- normalize_work_metadata(payload),
         :ok <- apply_validation(&RunnerTask.validate_payload/2, task_kind, nil, payload),
         :ok <- Limits.validate_payload(task_kind, payload),
         {:ok, stripped, hash} <- strip_package(payload),
         {:ok, data} <- PersistenceData.encode(stripped, Limits.payload_bytes(task_kind)) do
      envelope = %{
        "encoding" => @payload_encoding,
        "execution_package_hash" => hash,
        "payload" => data,
        "protocol_version" => @protocol_version,
        "task_kind" => Atom.to_string(task_kind),
        "type" => "runner_task_payload"
      }

      {:ok, hash} = payload_hash(envelope)
      {:ok, envelope, hash}
    end
  end

  defp normalize_work_metadata(%RuntimeInputResolutionRequest{work: work} = request) do
    with {:ok, work} <- normalize_work_metadata(work), do: {:ok, %{request | work: work}}
  end

  defp normalize_work_metadata(%RunnerWork{metadata: metadata} = work) when is_map(metadata) do
    with {:ok, operator_metadata} <-
           OpenData.normalize(Map.get(metadata, :operator_metadata, %{})) do
      metadata =
        if Map.has_key?(metadata, :operator_metadata),
          do: Map.put(metadata, :operator_metadata, operator_metadata),
          else: metadata

      {:ok, %{work | metadata: metadata}}
    else
      {:error, reason} -> {:error, {:invalid_runner_task_open_data, :operator_metadata, reason}}
    end
  end

  defp normalize_work_metadata(payload), do: {:ok, payload}

  def decode_payload(task_kind, envelope, version \\ nil, packages \\ []) do
    with {:ok, hash} <- package_hash(envelope),
         true <- envelope["task_kind"] == Atom.to_string(task_kind),
         {:ok, stripped} <-
           PersistenceData.decode(
             envelope["payload"],
             Limits.payload_bytes(task_kind),
             version,
             [],
             packages
           ),
         {:ok, value} <- restore_package(stripped, hash, packages),
         true <- required_package_present?(value, version),
         :ok <- apply_validation(&RunnerTask.validate_payload/2, task_kind, nil, value),
         :ok <- Limits.validate_payload(task_kind, value) do
      {:ok, value}
    else
      {:error, :invalid_runner_task_data} = error -> error
      _other -> {:error, :invalid_runner_task_persistence_envelope}
    end
  rescue
    _error -> {:error, :invalid_runner_task_persistence_envelope}
  end

  def encode_result(task_kind, outcome, result) do
    with {:ok, result} <- PersistenceResult.normalize(task_kind, result) do
      case encode("runner_task_result", task_kind, outcome, result, &RunnerTask.validate_result/3) do
        {:ok, envelope, _hash} -> {:ok, envelope}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def decode_result(task_kind, outcome, envelope, version \\ nil, packages \\ [])

  def decode_result(task_kind, outcome, nil, _version, _packages) do
    case RunnerTask.validate_result(task_kind, outcome, nil) do
      :ok -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode_result(task_kind, outcome, envelope, version, packages),
    do:
      decode(
        "runner_task_result",
        task_kind,
        outcome,
        envelope,
        &RunnerTask.validate_result/3,
        version,
        packages
      )

  # Read only a fixed, bounded path before loading the independently retained package.
  def package_hash(
        %{
          "encoding" => @payload_encoding,
          "execution_package_hash" => hash,
          "protocol_version" => @protocol_version,
          "type" => "runner_task_payload",
          "task_kind" => kind,
          "payload" => %{"format" => "task-data-v1", "data" => _} = inner
        } = envelope
      )
      when map_size(envelope) == 6 and map_size(inner) == 2 do
    cond do
      is_nil(hash) ->
        {:ok, nil}

      kind in ["asset_attempt", "runtime_input_resolution"] and is_binary(hash) and
        byte_size(hash) == 64 and
          Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) ->
        {:ok, hash}

      true ->
        {:error, :invalid_runner_task_package_reference}
    end
  end

  def package_hash(_), do: {:error, :invalid_runner_task_package_reference}

  defp strip_package(%RuntimeInputResolutionRequest{work: work} = request) do
    with {:ok, work, hash} <- strip_package(work), do: {:ok, %{request | work: work}, hash}
  end

  defp strip_package(%RunnerWork{execution_package: %ExecutionPackage{} = package} = work) do
    if RunnerWork.asset_ref(work) == package.asset_ref,
      do: {:ok, %{work | execution_package: nil}, package.content_hash},
      else: {:error, :invalid_runner_task_package_reference}
  end

  defp strip_package(value), do: {:ok, value, nil}

  defp restore_package(%RuntimeInputResolutionRequest{work: work} = request, hash, packages) do
    with {:ok, work} <- restore_package(work, hash, packages), do: {:ok, %{request | work: work}}
  end

  defp restore_package(%RunnerWork{execution_package: nil} = work, hash, [
         %ExecutionPackage{content_hash: hash} = package
       ])
       when is_binary(hash) do
    if RunnerWork.asset_ref(work) == package.asset_ref,
      do: {:ok, %{work | execution_package: package}},
      else: {:error, :invalid_runner_task_package_reference}
  end

  defp restore_package(%RunnerWork{execution_package: nil} = work, nil, []), do: {:ok, work}

  defp restore_package(%RunnerWork{}, _hash, _packages),
    do: {:error, :invalid_runner_task_package_reference}

  defp restore_package(value, nil, []), do: {:ok, value}

  defp restore_package(_value, _hash, _packages),
    do: {:error, :invalid_runner_task_package_reference}

  defp required_package_present?(%RunnerWork{execution_package: nil} = work, %Version{} = version) do
    case Enum.find(version.manifest.assets, &(&1.ref == RunnerWork.asset_ref(work))) do
      %{type: :sql} -> false
      _other -> true
    end
  end

  defp required_package_present?(_value, _version), do: true

  def payload_hash(envelope) when is_map(envelope),
    do: {:ok, :crypto.hash(:sha256, Serializer.encode_canonical!(envelope))}

  def payload_hash(_envelope), do: {:error, :invalid_runner_task_persistence_envelope}

  def hash_term(value),
    do: {:ok, :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic]))}

  defp encode(tag, task_kind, outcome, value, validate) do
    limit = term_limit(tag, task_kind)

    with :ok <- apply_validation(validate, task_kind, outcome, value) do
      size = byte_size(:erlang.term_to_binary(value, [:deterministic]))

      if size <= limit do
        with {:ok, data} <- PersistenceData.encode(value, limit) do
          envelope =
            %{
              "encoding" => "task-data-v1",
              "payload" => data,
              "protocol_version" => @protocol_version,
              "task_kind" => Atom.to_string(task_kind),
              "type" => tag
            }
            |> maybe_put_outcome(outcome)

          {:ok, hash} = payload_hash(envelope)
          {:ok, envelope, hash}
        end
      else
        {:error, {:runner_task_payload_too_large, size, limit}}
      end
    end
  end

  defp decode(tag, task_kind, outcome, envelope, validate, version, packages)
       when is_map(envelope) do
    limit = term_limit(tag, task_kind)

    expected =
      %{
        "encoding" => "task-data-v1",
        "protocol_version" => @protocol_version,
        "task_kind" => Atom.to_string(task_kind),
        "type" => tag
      }
      |> maybe_put_outcome(outcome)

    with true <- Map.drop(envelope, ["payload"]) == expected,
         {:ok, value} <-
           PersistenceData.decode(Map.get(envelope, "payload"), limit, version, [], packages),
         :ok <- apply_validation(validate, task_kind, outcome, value) do
      {:ok, value}
    else
      {:error, :invalid_runner_task_data} = error -> error
      _other -> {:error, :invalid_runner_task_persistence_envelope}
    end
  rescue
    _error -> {:error, :invalid_runner_task_persistence_envelope}
  end

  defp decode(_tag, _task_kind, _outcome, _envelope, _validate, _version, _packages),
    do: {:error, :invalid_runner_task_persistence_envelope}

  defp apply_validation(validate, task_kind, nil, value) do
    with :ok <- validate.(task_kind, value), do: PersistenceSchema.payload(task_kind, value)
  end

  defp apply_validation(validate, task_kind, outcome, value) do
    with :ok <- validate.(task_kind, outcome, value),
         do: PersistenceSchema.result(task_kind, outcome, value)
  end

  defp maybe_put_outcome(envelope, nil), do: envelope

  defp maybe_put_outcome(envelope, outcome),
    do: Map.put(envelope, "outcome", Atom.to_string(outcome))

  defp term_limit("runner_task_result", _kind), do: Limits.result_bytes()
end
