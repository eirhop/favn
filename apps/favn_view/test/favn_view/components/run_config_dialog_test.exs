defmodule FavnView.Components.RunConfigDialogTest do
  @moduledoc """
  Covers the three things the run dialog decides for itself.

  Everything else it renders is `FavnView.AssetRunConfig`'s answer, tested there. The
  design-system suite smoke-renders this component, which catches a raise and nothing
  else; these are the contracts its moduledoc actually states.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunConfigDialog
  alias FavnView.Dev.DesignSystem.Fixtures.RunConfig

  describe "the period fieldset" do
    test "is offered to an asset that runs one window at a time" do
      html = dialog(has_data_windows?: true)

      assert html =~ "Run period"
      assert html =~ ~s(data-testid="run-config-window-kind")
      assert html =~ ~s(data-testid="run-config-window-value")
    end

    # An asset with no window policy replaces its whole relation on every run, so there
    # is no period to choose and offering the fields would invite a rejected run.
    test "is absent from an asset that replaces its whole relation" do
      html = dialog(has_data_windows?: false, run_config: RunConfig.full_refresh_run_config())

      refute html =~ "Run period"
      refute html =~ ~s(data-testid="run-config-window-kind")
      refute html =~ ~s(data-testid="run-config-window-value")
      refute html =~ ~s(data-testid="run-config-window-timezone")
    end

    test "does not decide whether the rest of the dialog renders" do
      html = dialog(has_data_windows?: false, run_config: RunConfig.full_refresh_run_config())

      assert html =~ "What else runs"
      assert html =~ "Whether to rerun what is already current"
      assert html =~ ~s(data-testid="submit-run-config")
    end
  end

  describe "the upstream warning" do
    # "Force everything planned" forces whatever the plan holds, so with dependencies
    # included it reaches upstream assets and carries the same consequence as forcing
    # upstream explicitly.
    test "appears for both refresh modes that force an upstream asset" do
      for mode <- ~w(force_selected_upstream force_all) do
        assert dialog(run_config: with_refresh(mode, "all")) =~
                 "Forcing upstream assets changes their inputs",
               "#{mode} plans upstream and should warn"
      end
    end

    test "stays away from the modes that leave upstream alone" do
      for mode <- ~w(auto missing force_selected) do
        refute dialog(run_config: with_refresh(mode, "all")) =~
                 "Forcing upstream assets changes their inputs",
               "#{mode} should not warn about upstream"
      end
    end

    # With dependencies excluded the plan is this asset alone, so "force everything
    # planned" forces nothing upstream and the warning would describe a graph that is
    # not being run.
    test "stays away from forcing everything when no upstream asset is planned" do
      refute dialog(run_config: with_refresh("force_all", "none")) =~
               "Forcing upstream assets changes their inputs"
    end

    # Forcing upstream explicitly always plans upstream, because `AssetRunConfig` refuses
    # that mode without dependencies. So the warning stays even while the configuration is
    # invalid: the operator needs both the consequence and the reason it cannot be sent.
    test "stays on a forced upstream that excluded dependencies, alongside the error" do
      html =
        dialog(
          run_config: with_refresh("force_selected_upstream", "none"),
          run_config_valid?: false,
          error: "force_selected_upstream requires dependencies=all."
        )

      assert html =~ "Forcing upstream assets changes their inputs"
      assert html =~ "force_selected_upstream requires dependencies=all."
      assert submit_disabled?(html)
    end
  end

  describe "submitting" do
    test "is offered to an operator with a valid configuration" do
      html = dialog(can_submit_runs?: true, run_config_valid?: true)

      refute html =~ ~s(data-testid="run-config-not-permitted")
      refute submit_disabled?(html)
    end

    # A control that refuses without saying why reads as broken, and the dialog says it
    # in prose rather than leaving it to a title attribute that touch never shows.
    test "is refused and explained to a viewer" do
      html = dialog(can_submit_runs?: false)

      assert html =~ ~s(data-testid="run-config-not-permitted")
      assert html =~ "needs an operator account"
      assert submit_disabled?(html)
    end

    test "is refused while the configuration is invalid, with the reason shown" do
      html =
        dialog(
          can_submit_runs?: true,
          run_config_valid?: false,
          error: "force_selected_upstream requires dependencies=all."
        )

      assert submit_disabled?(html)
      assert html =~ ~s(data-testid="run-config-error")
      assert html =~ "force_selected_upstream requires dependencies=all."
    end

    test "is refused while a submission is already in flight" do
      html = dialog(can_submit_runs?: true, submitting_window_run?: true)

      assert submit_disabled?(html)
    end
  end

  defp with_refresh(mode, dependencies),
    do: RunConfig.run_config(:refresh_timeline, :day, "2026-06-12", dependencies, mode)

  defp dialog(overrides) do
    assigns =
      Keyword.merge(
        [
          has_data_windows?: true,
          run_config: RunConfig.default_run_config(),
          run_config_valid?: true,
          submitting_window_run?: false,
          error: nil,
          can_submit_runs?: true,
          command_resource: "asset:orders"
        ],
        overrides
      )

    render_component(&RunConfigDialog.run_config_dialog/1, assigns)
  end

  # The submit button is the last disabled control in the dialog, so the assertion has
  # to name it rather than look for "disabled" anywhere in the markup.
  defp submit_disabled?(html) do
    case Regex.run(~r/<button[^>]*data-testid="submit-run-config"[^>]*>/, html) do
      [tag] -> tag =~ "disabled"
      nil -> flunk("the dialog rendered no submit button")
    end
  end
end
