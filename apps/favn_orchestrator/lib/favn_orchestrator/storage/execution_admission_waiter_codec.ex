defmodule FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec do
  @moduledoc false

  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.Storage.PayloadCodec

  @spec normalize(map() | Waiter.t()) :: {:ok, Waiter.t()} | {:error, term()}
  def normalize(waiter), do: Waiter.normalize(waiter)

  @spec encode(map() | Waiter.t()) :: {:ok, binary()} | {:error, term()}
  def encode(waiter) do
    with {:ok, normalized} <- normalize(waiter) do
      normalized
      |> Map.from_struct()
      |> PayloadCodec.encode()
    end
  rescue
    exception -> {:error, {:invalid_execution_admission_waiter_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, Waiter.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, waiter} <- PayloadCodec.decode(payload) do
      normalize(waiter)
    end
  rescue
    exception -> {:error, {:invalid_execution_admission_waiter_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_execution_admission_waiter_payload}
end
