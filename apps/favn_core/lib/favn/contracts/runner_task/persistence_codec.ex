defmodule Favn.Contracts.RunnerTask.PersistenceCodec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerTask.PersistenceSchema
  alias Favn.Contracts.RunnerTask.PersistenceData
  alias Favn.Manifest.Serializer

  @protocol_version RunnerTask.version()
  alias Favn.Contracts.RunnerTask.Limits

  def encode_payload(task_kind, payload),
    do: encode("runner_task_payload", task_kind, nil, payload, &RunnerTask.validate_payload/2)

  def decode_payload(task_kind, envelope, version \\ nil, packages \\ []),
    do:
      decode(
        "runner_task_payload",
        task_kind,
        nil,
        envelope,
        &RunnerTask.validate_payload/2,
        version,
        packages
      )

  def encode_result(task_kind, outcome, result) do
    case encode("runner_task_result", task_kind, outcome, result, &RunnerTask.validate_result/3) do
      {:ok, envelope, _hash} -> {:ok, envelope}
      {:error, reason} -> {:error, reason}
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
          "task_kind" => "asset_attempt",
          "encoding" => "task-data-v1",
          "protocol_version" => @protocol_version,
          "type" => "runner_task_payload",
          "payload" => %{"format" => "task-data-v1", "data" => data} = inner
        } = envelope
      )
      when map_size(envelope) == 5 and map_size(inner) == 2 do
    with {:ok, fields} <- struct_fields(data, Favn.Contracts.RunnerWork),
         package <- Map.fetch!(fields, "execution_package") do
      case package do
        nil ->
          {:ok, nil}

        _ ->
          with {:ok, fields} <- struct_fields(package, Favn.Manifest.ExecutionPackage),
               ["binary", encoded] when byte_size(encoded) == 88 <- fields["content_hash"],
               {:ok, hash} <- Base.decode64(encoded),
               true <- Base.encode64(hash) == encoded and Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) do
            {:ok, hash}
          else
            _ -> {:error, :invalid_runner_task_package_reference}
          end
      end
    else
      _ -> {:error, :invalid_runner_task_package_reference}
    end
  end

  def package_hash(%{"task_kind" => kind}) when kind != "asset_attempt", do: {:ok, nil}
  def package_hash(_), do: {:error, :invalid_runner_task_package_reference}

  defp struct_fields(["struct", name, ["map", pairs]], module) when is_list(pairs) do
    expected =
      Map.from_struct(struct(module)) |> Map.keys() |> Enum.map(&Atom.to_string/1) |> Enum.sort()

    with true <- name == Atom.to_string(module),
         true <- length(pairs) == length(expected),
         true <- Enum.all?(pairs, &match?([["atom", key], _] when is_binary(key), &1)),
         keys <- Enum.map(pairs, fn [["atom", key], _] -> key end),
         true <- Enum.sort(keys) == expected do
      {:ok, Map.new(pairs, fn [["atom", key], value] -> {key, value} end)}
    else
      _ -> {:error, :invalid_runner_task_package_reference}
    end
  end

  defp struct_fields(_, _), do: {:error, :invalid_runner_task_package_reference}

  defp package_matches?(
         "runner_task_payload",
         %Favn.Contracts.RunnerWork{execution_package: package} = work,
         packages
       ) do
    case {package, packages} do
      {nil, []} ->
        true

      {%Favn.Manifest.ExecutionPackage{} = package, [package]} ->
        Favn.Contracts.RunnerWork.asset_ref(work) == package.asset_ref

      _ ->
        false
    end
  end

  defp package_matches?(_tag, _value, _packages), do: true

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
         :ok <- apply_validation(validate, task_kind, outcome, value),
         true <- package_matches?(tag, value, packages) do
      {:ok, value}
    else
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

  defp term_limit("runner_task_payload", kind), do: Limits.payload_bytes(kind)
  defp term_limit("runner_task_result", _kind), do: Limits.result_bytes()
end
