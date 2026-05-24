defmodule FavnView.ScheduleRoute do
  @moduledoc false

  @prefix "s-"

  def to_param(schedule_id) when is_binary(schedule_id) do
    @prefix <> Base.url_encode64(schedule_id, padding: false)
  end

  def from_param(@prefix <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, schedule_id} -> schedule_id
      :error -> @prefix <> encoded
    end
  end

  def from_param(param), do: param
end
