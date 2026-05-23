defmodule FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec do
  @moduledoc false

  alias FavnOrchestrator.ExecutionAdmission.Waiter

  @spec normalize(map() | Waiter.t()) :: {:ok, Waiter.t()} | {:error, term()}
  def normalize(waiter), do: Waiter.normalize(waiter)

  @spec encode(map() | Waiter.t()) :: {:ok, binary()} | {:error, term()}
  def encode(waiter) do
    with {:ok, normalized} <- normalize(waiter) do
      {:ok, Base.encode64(:erlang.term_to_binary(normalized))}
    end
  rescue
    exception -> {:error, {:invalid_execution_admission_waiter_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, Waiter.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    payload
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
    |> normalize()
  rescue
    exception -> {:error, {:invalid_execution_admission_waiter_payload, exception}}
  end
end
