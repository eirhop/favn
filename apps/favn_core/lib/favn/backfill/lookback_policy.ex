defmodule Favn.Backfill.LookbackPolicy do
  @moduledoc """
  Normalizes operator lookback policy values for operational backfills.

  A lookback policy is either `:asset_default`, meaning the asset's compiled
  window lookback should be used, or a concrete non-negative integer count.
  """

  @type t :: :asset_default | non_neg_integer()

  @doc """
  Normalizes a lookback policy value.

  Accepted values are `:asset_default`, `"asset-default"`, `"asset_default"`,
  non-negative integers, and numeric strings representing non-negative integers.
  """
  @spec normalize(term()) :: {:ok, t()} | {:error, {:invalid_lookback_policy, term()}}
  def normalize(:asset_default), do: {:ok, :asset_default}
  def normalize("asset-default"), do: {:ok, :asset_default}
  def normalize("asset_default"), do: {:ok, :asset_default}
  def normalize(value) when is_integer(value) and value >= 0, do: {:ok, value}

  def normalize(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _other -> {:error, {:invalid_lookback_policy, value}}
    end
  end

  def normalize(value), do: {:error, {:invalid_lookback_policy, value}}
end
