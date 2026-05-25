defmodule FavnOrchestrator.Storage.ExecutionOwnershipCodec do
  @moduledoc false

  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.Storage.PayloadCodec

  @spec normalize(RunExecutionOwnership.t() | map()) ::
          {:ok, RunExecutionOwnership.t()} | {:error, term()}
  def normalize(%RunExecutionOwnership{} = ownership), do: {:ok, ownership}

  def normalize(%{} = ownership), do: RunExecutionOwnership.from_map(ownership)
  def normalize(_ownership), do: {:error, :invalid_execution_ownership}

  @spec encode(RunExecutionOwnership.t() | map()) :: {:ok, binary()} | {:error, term()}
  def encode(ownership) do
    with {:ok, normalized} <- normalize(ownership) do
      normalized
      |> RunExecutionOwnership.to_map()
      |> PayloadCodec.encode()
    end
  rescue
    exception -> {:error, {:invalid_execution_ownership_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, RunExecutionOwnership.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, map} <- PayloadCodec.decode(payload) do
      RunExecutionOwnership.from_map(map)
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, {:invalid_execution_ownership_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_execution_ownership_payload}
end
