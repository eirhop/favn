defmodule Favn.Contracts.RunnerTask.Limits do
  @moduledoc false

  alias Favn.Contracts.RunnerTask.Assignment

  @default_bytes 1_048_576
  @asset_payload_bytes 8 * @default_bytes
  @assignment_bytes @asset_payload_bytes + 65_536

  @spec payload_bytes(atom()) :: pos_integer()
  def payload_bytes(:asset_attempt), do: @asset_payload_bytes
  def payload_bytes(_kind), do: @default_bytes

  @spec result_bytes() :: pos_integer()
  def result_bytes, do: @default_bytes

  @spec assignment_bytes() :: pos_integer()
  def assignment_bytes, do: @assignment_bytes

  @spec encoded_bytes(non_neg_integer()) :: non_neg_integer()
  def encoded_bytes(raw_bytes), do: 4 * div(raw_bytes + 2, 3)

  @spec wire_bytes(module()) :: pos_integer()
  def wire_bytes(Assignment),
    do: encoded_bytes(@assignment_bytes)

  def wire_bytes(_module), do: 2 * @default_bytes

  @spec validate_payload(atom(), term()) :: :ok | {:error, term()}
  def validate_payload(kind, payload) do
    size = payload |> :erlang.term_to_binary([:deterministic]) |> byte_size()
    limit = payload_bytes(kind)
    if size <= limit, do: :ok, else: {:error, {:runner_task_payload_too_large, size, limit}}
  end
end
