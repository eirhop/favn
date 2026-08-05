defmodule FavnView.AssetRunConfigTest do
  @moduledoc """
  Covers the six rules that decide whether the run dialog may submit.

  The dialog's error strings are rendered by design-system fixtures, so before this
  file the messages were demonstrated by a fixture and produced by untested code.
  """

  use ExUnit.Case, async: true

  doctest FavnView.AssetRunConfig

  alias FavnView.AssetRunConfig

  describe "a configuration with no period" do
    test "is valid, because an asset that replaces its whole relation submits none" do
      assert AssetRunConfig.validate(AssetRunConfig.default()) == nil
      refute AssetRunConfig.window_context_requested?(AssetRunConfig.default())
    end

    test "is still checked for the rules that do not concern a period" do
      assert AssetRunConfig.validate(config(dependencies: "sideways")) ==
               "Dependency choice is invalid."

      assert AssetRunConfig.validate(config(refresh: "whenever")) ==
               "Refresh choice is invalid."
    end

    # #595 changed this: the window-context rules used to be skipped whenever a window
    # was selected on screen, and now apply to whatever the form actually carries.
    test "does not have its period fields validated" do
      # No value and no `to`, so an invalid kind, a missing source, and a nonsense
      # timezone are all beside the point.
      assert AssetRunConfig.validate(
               config(kind: "fortnight", source: nil, timezone: "not a zone")
             ) == nil
    end
  end

  describe "a configuration that asks for a period" do
    test "is requested by either bound" do
      assert AssetRunConfig.window_context_requested?(config(value: "2026-07-01"))
      assert AssetRunConfig.window_context_requested?(config(to: "2026-07-03"))
      refute AssetRunConfig.window_context_requested?(config(value: "   ", to: ""))
    end

    test "needs a source the backend named" do
      assert AssetRunConfig.validate(period(source: nil)) == "Window source is invalid."

      assert AssetRunConfig.validate(period(source: "guessed")) ==
               "Window source is invalid."

      assert AssetRunConfig.validate(period(source: "data_coverage_timeline")) == nil
    end

    test "needs a kind the backend understands" do
      assert AssetRunConfig.validate(period(kind: "")) == "Window kind is invalid."
      assert AssetRunConfig.validate(period(kind: "fortnight")) == "Window kind is invalid."

      for kind <- ~w(hour day month year) do
        assert AssetRunConfig.validate(period(kind: kind)) == nil
      end
    end

    # `to` alone asks for a period, so a range with only an end has no start to run from.
    test "needs a start even when only its end was filled in" do
      assert AssetRunConfig.validate(period(value: "", to: "2026-07-03")) ==
               "Window range start is required."

      assert AssetRunConfig.validate(period(value: "  ", to: "2026-07-03")) ==
               "Window range start is required."
    end

    test "needs a timezone shaped like an identifier" do
      assert AssetRunConfig.validate(period(timezone: "Europe/Oslo")) == nil
      assert AssetRunConfig.validate(period(timezone: "Etc/GMT+2")) == nil
      assert AssetRunConfig.validate(period(timezone: "")) == "Timezone is invalid."
      assert AssetRunConfig.validate(period(timezone: "Europe Oslo")) == "Timezone is invalid."

      assert AssetRunConfig.validate(period(timezone: String.duplicate("z", 65))) ==
               "Timezone is invalid."
    end
  end

  describe "forcing upstream" do
    # The dialog offers "only this asset" and "force this asset and its upstream"
    # independently, so the combination is reachable by clicking. The backend refuses
    # it in `SubmissionBuilder.validate_refresh_policy_dependencies/2`; refusing it here
    # is what turns that rejection into an explanation.
    test "cannot be asked for while dependencies are excluded" do
      assert AssetRunConfig.validate(
               config(dependencies: "none", refresh: "force_selected_upstream")
             ) == "force_selected_upstream requires dependencies=all."
    end

    test "is allowed with dependencies, and every other refresh mode is allowed without" do
      assert AssetRunConfig.validate(
               config(dependencies: "all", refresh: "force_selected_upstream")
             ) == nil

      for refresh <- ~w(auto missing force_selected force_all) do
        assert AssetRunConfig.validate(config(dependencies: "none", refresh: refresh)) == nil
      end
    end
  end

  describe "reading a configuration from the backend" do
    test "turns the period an asset is due for into form strings" do
      config =
        AssetRunConfig.from_asset(%{
          default_run_config: %{
            source: :refresh_timeline,
            kind: :day,
            value: "2026-07-01",
            timezone: "Europe/Oslo",
            dependencies: :all,
            refresh: :auto
          }
        })

      assert config == %{
               dependencies: "all",
               refresh: "auto",
               source: "refresh_timeline",
               kind: "day",
               value: "2026-07-01",
               to: "",
               timezone: "Europe/Oslo"
             }

      assert AssetRunConfig.validate(config) == nil
    end

    test "falls back to the empty default when no pipeline owns the asset" do
      assert AssetRunConfig.from_asset(%{default_run_config: nil}) == AssetRunConfig.default()
      assert AssetRunConfig.from_asset(nil) == AssetRunConfig.default()
    end

    test "normalises the refresh modes the backend spells differently" do
      for {reported, expected} <- [
            {:force, "force_all"},
            {:force_all, "force_all"},
            {:force_selected, "force_selected"},
            {:force_selected_upstream, "force_selected_upstream"},
            {:missing, "missing"},
            {:auto, "auto"},
            {:something_new, "auto"}
          ] do
        assert AssetRunConfig.from_asset(%{default_run_config: %{refresh: reported}}).refresh ==
                 expected
      end
    end

    test "drops a source and kind it does not recognise rather than passing them on" do
      config = AssetRunConfig.from_asset(%{default_run_config: %{source: :invented, kind: :week}})

      assert config.source == nil
      assert config.kind == ""
    end
  end

  describe "applying a form change" do
    test "keeps the period fields the form did not send" do
      current = period(to: "2026-07-03", timezone: "Europe/Oslo")

      changed = AssetRunConfig.from_params(%{"run_config" => %{"kind" => "month"}}, current)

      assert changed.kind == "month"
      assert changed.value == current.value
      assert changed.to == current.to
      assert changed.timezone == current.timezone
      assert changed.source == current.source
    end

    # The two radio groups are the exception, and deliberately so: an unchecked group
    # sends nothing, and "no answer" there means the default rather than whatever the
    # previous answer happened to be.
    test "returns the radio groups to their default when the form sends neither" do
      current = config(dependencies: "none", refresh: "force_all")

      changed = AssetRunConfig.from_params(%{"run_config" => %{"kind" => "day"}}, current)

      assert changed.dependencies == "all"
      assert changed.refresh == "auto"
    end

    test "takes the radio groups from the form when it does send them" do
      changed =
        AssetRunConfig.from_params(
          %{"run_config" => %{"dependencies" => "none", "refresh" => "missing"}},
          AssetRunConfig.default()
        )

      assert changed.dependencies == "none"
      assert changed.refresh == "missing"
    end

    test "returns the current configuration when the change carries none" do
      current = period()

      assert AssetRunConfig.from_params(%{}, current) == current
      assert AssetRunConfig.from_params(%{"run_config" => "not a map"}, current) == current
    end

    # A form cannot be trusted to send only the values the dialog rendered.
    test "does not sanitise, so a tampered field is caught by validation instead" do
      changed =
        AssetRunConfig.from_params(
          %{"run_config" => %{"dependencies" => "everything"}},
          AssetRunConfig.default()
        )

      assert changed.dependencies == "everything"
      assert AssetRunConfig.validate(changed) == "Dependency choice is invalid."
    end
  end

  describe "asking for a range" do
    # The submission path branches on this, and blankness has to mean the same thing
    # here as it does in validation or a configuration passes one and trips the other.
    test "needs an end bound that is not blank" do
      refute AssetRunConfig.range_requested?(AssetRunConfig.default())
      refute AssetRunConfig.range_requested?(period())
      refute AssetRunConfig.range_requested?(period(to: "   "))
      refute AssetRunConfig.range_requested?(period(to: nil))
      assert AssetRunConfig.range_requested?(period(to: "2026-07-03"))
    end

    # A whitespace-only end is not a range, so nothing about the period is missing.
    test "does not make a whitespace-only end into a validation failure" do
      assert AssetRunConfig.validate(period(to: "   ")) == nil
      refute AssetRunConfig.window_context_requested?(config(value: "", to: "   "))
    end
  end

  defp config(overrides), do: Map.merge(AssetRunConfig.default(), Map.new(overrides))

  defp period(overrides \\ []) do
    config(
      Keyword.merge(
        [source: "refresh_timeline", kind: "day", value: "2026-07-01", timezone: "Etc/UTC"],
        overrides
      )
    )
  end
end
