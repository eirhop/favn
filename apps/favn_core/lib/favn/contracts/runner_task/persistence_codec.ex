defmodule Favn.Contracts.RunnerTask.PersistenceCodec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask

  @protocol_version RunnerTask.version()
  alias Favn.Contracts.RunnerTask.Limits
  @max_orchestration_context_bytes 4 * 1_048_576

  def encode_payload(task_kind, payload),
    do: encode("runner_task_payload", task_kind, nil, payload, &RunnerTask.validate_payload/2)

  def decode_payload(task_kind, envelope),
    do: decode("runner_task_payload", task_kind, nil, envelope, &RunnerTask.validate_payload/2)

  def encode_result(task_kind, outcome, result) do
    case encode("runner_task_result", task_kind, outcome, result, &RunnerTask.validate_result/3) do
      {:ok, envelope, _hash} -> {:ok, envelope}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode_result(task_kind, outcome, nil) do
    case RunnerTask.validate_result(task_kind, outcome, nil) do
      :ok -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode_result(task_kind, outcome, envelope),
    do:
      decode(
        "runner_task_result",
        task_kind,
        outcome,
        envelope,
        &RunnerTask.validate_result/3
      )

  def encode_orchestration_context(context) when is_map(context),
    do: encode_private("runner_task_orchestration_context", context)

  def encode_orchestration_context(_context),
    do: {:error, :invalid_runner_task_orchestration_context}

  def decode_orchestration_context(envelope),
    do: decode_private("runner_task_orchestration_context", envelope)

  def payload_hash(envelope) when is_map(envelope),
    do: hash_term(envelope)

  def payload_hash(_envelope), do: {:error, :invalid_runner_task_persistence_envelope}

  def hash_term(value),
    do: {:ok, :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic]))}

  defp encode_private(tag, value) do
    binary = :erlang.term_to_binary(value, [:deterministic])

    if byte_size(binary) <= @max_orchestration_context_bytes do
      {:ok,
       %{
         "encoding" => "erlang-term-base64",
         "payload" => Base.encode64(binary),
         "protocol_version" => @protocol_version,
         "type" => tag
       }}
    else
      {:error,
       {:runner_task_orchestration_context_too_large, byte_size(binary),
        @max_orchestration_context_bytes}}
    end
  end

  defp decode_private(tag, envelope) when is_map(envelope) do
    with %{
           "encoding" => "erlang-term-base64",
           "payload" => payload,
           "protocol_version" => @protocol_version,
           "type" => ^tag
         } <- envelope,
         true <-
           Map.keys(envelope) |> Enum.sort() ==
             Enum.sort(~w(encoding payload protocol_version type)),
         true <-
           is_binary(payload) and
             byte_size(payload) <= encoded_limit(@max_orchestration_context_bytes),
         {:ok, binary} <- Base.decode64(payload),
         true <- byte_size(binary) <= @max_orchestration_context_bytes,
         {:ok, value} <- decode_uncompressed_term(binary),
         true <- is_map(value) do
      {:ok, value}
    else
      _other -> {:error, :invalid_runner_task_orchestration_context}
    end
  rescue
    _error -> {:error, :invalid_runner_task_orchestration_context}
  end

  defp decode_private(_tag, _envelope),
    do: {:error, :invalid_runner_task_orchestration_context}

  defp encode(tag, task_kind, outcome, value, validate) do
    limit = term_limit(tag, task_kind)

    with :ok <- apply_validation(validate, task_kind, outcome, value) do
      binary = :erlang.term_to_binary(value, [:deterministic])

      if byte_size(binary) <= limit do
        envelope =
          %{
            "encoding" => "erlang-term-base64",
            "payload" => Base.encode64(binary),
            "protocol_version" => @protocol_version,
            "task_kind" => Atom.to_string(task_kind),
            "type" => tag
          }
          |> maybe_put_outcome(outcome)

        {:ok, hash} = payload_hash(envelope)
        {:ok, envelope, hash}
      else
        {:error, {:runner_task_payload_too_large, byte_size(binary), limit}}
      end
    end
  end

  defp decode(tag, task_kind, outcome, envelope, validate) when is_map(envelope) do
    limit = term_limit(tag, task_kind)

    expected =
      %{
        "encoding" => "erlang-term-base64",
        "protocol_version" => @protocol_version,
        "task_kind" => Atom.to_string(task_kind),
        "type" => tag
      }
      |> maybe_put_outcome(outcome)

    with true <- Map.drop(envelope, ["payload"]) == expected,
         payload when is_binary(payload) <- Map.get(envelope, "payload"),
         true <- byte_size(payload) <= encoded_limit(limit),
         {:ok, binary} <- Base.decode64(payload),
         true <- byte_size(binary) <= limit,
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

  defp decode_uncompressed_term(<<131, 80, _rest::binary>>),
    do: {:error, :compressed_runner_task_payload_not_allowed}

  defp decode_uncompressed_term(<<131, _rest::binary>> = binary),
    do: {:ok, :erlang.binary_to_term(binary, [:safe])}

  defp decode_uncompressed_term(_binary), do: {:error, :invalid_runner_task_payload}

  defp maybe_put_outcome(envelope, nil), do: envelope

  defp maybe_put_outcome(envelope, outcome),
    do: Map.put(envelope, "outcome", Atom.to_string(outcome))

  defp term_limit("runner_task_payload", kind), do: Limits.payload_bytes(kind)
  defp term_limit("runner_task_result", _kind), do: Limits.result_bytes()

  defp encoded_limit(max_bytes), do: Limits.encoded_bytes(max_bytes)
end
