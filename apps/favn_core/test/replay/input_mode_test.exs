defmodule Favn.Replay.InputModeTest do
  use ExUnit.Case, async: true

  alias Favn.Replay.InputMode

  test "published operation defaults remain aligned" do
    assert InputMode.default_for(:manual) == :fresh
    assert InputMode.default_for(:scheduled) == :fresh
    assert InputMode.default_for(:backfill_child) == :fresh
    assert InputMode.default_for(:exact_replay) == :pinned
    assert InputMode.default_for(:resume_from_failure) == :inherit
    assert InputMode.default_for(:retry_remaining) == :inherit
    assert InputMode.values() == [:pinned, :inherit, :fresh]

    assert InputMode.operation_defaults() == [
             manual: :fresh,
             scheduled: :fresh,
             backfill_child: :fresh,
             fresh_rerun: :fresh,
             exact_replay: :pinned,
             resume_from_failure: :inherit,
             retry_remaining: :inherit
           ]
  end

  test "canonical guide operation matrix matches runtime defaults" do
    guide =
      __DIR__
      |> Path.join("../../../favn/guides/retries-and-replay.md")
      |> Path.expand()
      |> File.read!()

    labels = %{
      manual: "normal manual run",
      scheduled: "normal scheduled run",
      backfill_child: "normal backfill child",
      fresh_rerun: "fresh rerun",
      exact_replay: "exact replay",
      resume_from_failure: "resume from failure",
      retry_remaining: "retry remaining"
    }

    Enum.each(InputMode.operation_defaults(), fn {operation, mode} ->
      assert guide =~ "| #{Map.fetch!(labels, operation)} | `:#{mode}` |"
    end)
  end
end
