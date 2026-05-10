defmodule FavnView.AssetRoute do
  @moduledoc false

  @prefix "t-"

  def to_param(target_id) when is_binary(target_id) do
    @prefix <> Base.url_encode64(target_id, padding: false)
  end

  def from_param(@prefix <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, target_id} -> target_id
      :error -> @prefix <> encoded
    end
  end

  def from_param(param), do: param
end
