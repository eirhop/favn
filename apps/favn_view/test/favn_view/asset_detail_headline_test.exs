defmodule FavnView.AssetDetailHeadlineTest do
  use ExUnit.Case, async: true

  alias FavnView.AssetDetailLive

  doctest AssetDetailLive, only: [headline_status: 1, coverage_status: 1, blocks_writes?: 1]

  describe "headline_status/1" do
    test "a failed run outranks everything else the page knows" do
      detail = %{
        status: :failed,
        coverage: %{status: :incomplete},
        compatibility: %{blocks_writes?: true}
      }

      assert AssetDetailLive.headline_status(detail) == %{
               label: "Last run failed",
               tone: :error
             }
    end

    test "incomplete coverage outranks a healthy last run" do
      detail = %{status: :healthy, coverage: %{status: :incomplete}}

      assert %{label: "Coverage incomplete", tone: :warning} =
               AssetDetailLive.headline_status(detail)
    end

    test "blocked writes outrank a healthy last run" do
      detail = %{
        status: :healthy,
        coverage: %{status: :complete},
        compatibility: %{blocks_writes?: true}
      }

      assert %{label: "Writes blocked", tone: :warning} =
               AssetDetailLive.headline_status(detail)
    end

    test "healthy with unreported coverage does not claim to be healthy overall" do
      assert %{label: "Last run ok", tone: :success} =
               AssetDetailLive.headline_status(%{status: :healthy})
    end

    test "a running asset is not reported as an error" do
      assert %{label: "Running", tone: :warning} =
               AssetDetailLive.headline_status(%{status: :running})
    end

    test "an unrecognised status is neutral rather than a guess" do
      assert %{label: "Unknown", tone: :neutral} =
               AssetDetailLive.headline_status(%{status: :something_new})
    end
  end

  describe "blocks_writes?/1" do
    test "absent compatibility does not block on its own" do
      refute AssetDetailLive.blocks_writes?(%{})
      refute AssetDetailLive.blocks_writes?(%{compatibility: %{}})
      refute AssetDetailLive.blocks_writes?(%{compatibility: %{blocks_writes?: false}})
    end
  end
end
