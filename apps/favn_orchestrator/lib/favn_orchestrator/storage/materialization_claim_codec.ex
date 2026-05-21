defmodule FavnOrchestrator.Storage.MaterializationClaimCodec do
  @moduledoc false

  alias FavnOrchestrator.MaterializationClaim

  @spec normalize(MaterializationClaim.t() | map()) ::
          {:ok, MaterializationClaim.t()} | {:error, term()}
  def normalize(%MaterializationClaim{} = claim),
    do: MaterializationClaim.new(Map.from_struct(claim))

  def normalize(claim) when is_map(claim), do: MaterializationClaim.new(claim)
  def normalize(_claim), do: {:error, :invalid_materialization_claim}

  @spec encode(MaterializationClaim.t() | map()) :: {:ok, binary()} | {:error, term()}
  def encode(claim) do
    with {:ok, normalized} <- normalize(claim) do
      {:ok, Base.encode64(:erlang.term_to_binary(normalized))}
    end
  rescue
    exception -> {:error, {:invalid_materialization_claim_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, MaterializationClaim.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    payload
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
    |> normalize()
  rescue
    exception -> {:error, {:invalid_materialization_claim_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_materialization_claim_payload}
end
