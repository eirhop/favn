defmodule FavnOrchestrator.Storage.PayloadCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.PayloadCodec

  test "round-trips tagged runtime payload values" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    payload = %{
      asset_ref: {MyApp.Asset, :asset},
      status: :running,
      happened_at: now,
      nested: [%{reason: {:cancelled, :operator}}],
      scheduler: %Favn.Scheduler.State{
        pipeline_module: MyApp.Pipeline,
        schedule_id: :daily,
        version: 2,
        last_due_at: now
      }
    }

    assert {:ok, encoded} = PayloadCodec.encode(payload)
    assert encoded =~ "json-v1"
    assert encoded =~ "Elixir.MyApp.Asset"

    assert {:ok, decoded} = PayloadCodec.decode(encoded)
    assert decoded == payload
  end
end
