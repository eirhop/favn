defmodule FavnStoragePostgres.RunnerTasks.Codec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask
  alias FavnStoragePostgres.CanonicalJSON

  @protocol_version RunnerTask.version()
  @max_term_bytes 1_048_576

  @spec encode_payload(atom(), term()) :: {:ok, map(), binary()} | {:error, term()}
  def encode_payload(task_kind, payload) do
    encode("runner_task_payload", task_kind, nil, payload, &RunnerTask.validate_payload/2)
  end

  @spec decode_payload(atom(), map()) :: {:ok, term()} | {:error, term()}
  def decode_payload(task_kind, envelope) do
    decode("runner_task_payload", task_kind, nil, envelope, &RunnerTask.validate_payload/2)
  end

  @spec encode_result(atom(), atom(), term()) :: {:ok, map()} | {:error, term()}
  def encode_result(task_kind, outcome, result) do
    case encode(
           "runner_task_result",
           task_kind,
           outcome,
           result,
           &RunnerTask.validate_result/3
         ) do
      {:ok, envelope, _hash} -> {:ok, envelope}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec decode_result(atom(), atom(), map() | nil) :: {:ok, term()} | {:error, term()}
  def decode_result(task_kind, outcome, nil) do
    case RunnerTask.validate_result(task_kind, outcome, nil) do
      :ok -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode_result(task_kind, outcome, envelope) do
    decode(
      "runner_task_result",
      task_kind,
      outcome,
      envelope,
      &RunnerTask.validate_result/3
    )
  end

  @spec payload_hash(map()) :: {:ok, binary()} | {:error, term()}
  def payload_hash(envelope), do: CanonicalJSON.hash(envelope)

  defp encode(tag, task_kind, outcome, value, validate) do
    with :ok <- apply_validation(validate, task_kind, outcome, value) do
      binary = :erlang.term_to_binary(value, [:deterministic])

      if byte_size(binary) <= @max_term_bytes do
        envelope = %{
          "encoding" => "erlang-term-base64",
          "payload" => Base.encode64(binary),
          "protocol_version" => @protocol_version,
          "task_kind" => Atom.to_string(task_kind),
          "type" => tag
        }

        envelope =
          if outcome,
            do: Map.put(envelope, "outcome", Atom.to_string(outcome)),
            else: envelope

        with {:ok, hash} <- CanonicalJSON.hash(envelope) do
          {:ok, envelope, hash}
        end
      else
        {:error, {:runner_task_payload_too_large, byte_size(binary), @max_term_bytes}}
      end
    end
  end

  defp decode(tag, task_kind, outcome, envelope, validate) when is_map(envelope) do
    expected =
      %{
        "encoding" => "erlang-term-base64",
        "protocol_version" => @protocol_version,
        "task_kind" => Atom.to_string(task_kind),
        "type" => tag
      }
      |> maybe_put_outcome(outcome)

    with true <- envelope_metadata(envelope) == expected,
         payload when is_binary(payload) <- Map.get(envelope, "payload"),
         true <- byte_size(payload) <= encoded_limit(),
         {:ok, binary} <- Base.decode64(payload),
         true <- byte_size(binary) <= @max_term_bytes,
         {:ok, value} <- decode_uncompressed_term(binary),
         :ok <- apply_validation(validate, task_kind, outcome, value) do
      {:ok, value}
    else
      _other -> {:error, :invalid_runner_task_persistence_envelope}
    end
  rescue
    _error -> {:error, :invalid_runner_task_persistence_envelope}
  end

  defp decode(_tag, _task_kind, _outcome, _envelope, _validate),
    do: {:error, :invalid_runner_task_persistence_envelope}

  defp apply_validation(validate, task_kind, nil, value), do: validate.(task_kind, value)

  defp apply_validation(validate, task_kind, outcome, value),
    do: validate.(task_kind, outcome, value)

  defp envelope_metadata(envelope), do: Map.drop(envelope, ["payload"])

  defp decode_uncompressed_term(<<131, 80, _rest::binary>>),
    do: {:error, :compressed_runner_task_payload_not_allowed}

  defp decode_uncompressed_term(<<131, _rest::binary>> = binary),
    do: {:ok, :erlang.binary_to_term(binary, [:safe])}

  defp decode_uncompressed_term(_binary), do: {:error, :invalid_runner_task_payload}

  defp maybe_put_outcome(envelope, nil), do: envelope

  defp maybe_put_outcome(envelope, outcome),
    do: Map.put(envelope, "outcome", Atom.to_string(outcome))

  defp encoded_limit, do: div(@max_term_bytes * 4, 3) + 4
end
