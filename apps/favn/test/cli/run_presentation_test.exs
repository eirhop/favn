defmodule Favn.CLI.RunPresentationTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.RunPresentation

  test "prefers the persisted pipeline label" do
    run = %{
      "target_label" => "Elixir.Example.Pipeline:daily",
      "target_refs" => ["Elixir.Example.Asset:orders"]
    }

    assert RunPresentation.target_label(run) == "Elixir.Example.Pipeline:daily"
  end

  test "falls back to persisted target refs for asset runs" do
    run = %{target_refs: ["Elixir.Example.Asset:orders", "Elixir.Example.Asset:customers"]}

    assert RunPresentation.target_label(run) ==
             "Elixir.Example.Asset:orders,Elixir.Example.Asset:customers"
  end
end
