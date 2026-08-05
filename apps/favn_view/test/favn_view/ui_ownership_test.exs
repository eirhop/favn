defmodule FavnView.UIOwnershipTest do
  @moduledoc """
  Elements own their type steps and their padding. Pages pick which element to use.

  These are the two contracts #595 proved are not self-enforcing: it moved the eyebrow
  step from `text-xs` to `text-sm` and had to hand-edit seventeen literal copies to keep
  them in step, and it grew an invented panel padding from four call sites to fifteen.
  Both are drift that stays invisible until a change to the element fails to reach the
  copies.
  """

  use ExUnit.Case, async: true

  alias FavnView.UI.Typography

  @view_root Path.join([__DIR__, "..", ".."])

  describe "the eyebrow type step" do
    # Only this step. `:meta` and `:body` are both `text-sm favn-text-muted`, a pair of
    # ordinary utilities that says nothing about intent when written directly; treating
    # every occurrence as a copy would be noise. The eyebrow's value carries an arbitrary
    # tracking value, so a literal copy of it is unambiguous — and it is the step that
    # actually drifted.
    test "appears only in the module that defines it" do
      for file <- styled_files(), path = Path.relative_to_cwd(file) do
        refute String.contains?(File.read!(file), Typography.class(:eyebrow)),
               "#{path} copies the eyebrow type step; call <.eyebrow> where a <p> suits, " <>
                 "or Typography.class(:eyebrow) where the tag must differ"
      end
    end

    # Every subtle uppercase label in the same role now takes this one tracking value. The
    # scope is `favn-text-subtle`, which already leaves out the two uppercase runs that are
    # not this step: a bold `text-base-content` heading in the runs list and a
    # terminal-coloured badge in the log viewer.
    #
    # The login wordmark is excluded by name. "Favn Operator" over the sign-in form is a
    # brand lockup, and its wider tracking is the point; it is not a label above a value.
    @wordmark "controllers/operator_session_html/new.html.heex"

    test "is the only tracking value any subtle uppercase label uses" do
      offenders =
        for file <- styled_files(),
            not String.ends_with?(file, @wordmark),
            line <- String.split(File.read!(file), "\n"),
            line =~ ~r/uppercase[^"]*tracking-\[0\.\d+em\]/,
            not (line =~ ~r/tracking-\[0\.18em\]/),
            line =~ "favn-text-subtle",
            do: Path.relative_to_cwd(file) <> ": " <> String.trim(line)

      assert offenders == [],
             "these invent their own tracking for a label the eyebrow step already " <>
               "covers:\n" <> Enum.join(offenders, "\n")
    end
  end

  describe "panel padding" do
    # Narrow on purpose: this catches `padding={:none}` carrying a padding utility, which
    # is how the invented step was written every time it appeared. It does not parse HEEx,
    # so it does not see `<.panel class="p-8">` with no `padding` attribute, nor a utility
    # separated from the attribute by more than a couple of others. The named steps in
    # `FavnView.UI.Surface` are the control; this is a tripwire for the shape that recurred.
    test "is never decided in a page beside padding={:none}" do
      pattern = ~r/padding=\{:none\}[^>]{0,120}?\bp-(?:\d|\[)/s

      for file <- styled_files(), path = Path.relative_to_cwd(file) do
        refute File.read!(file) =~ pattern,
               "#{path} decides a panel's padding in a page; add the step to " <>
                 "FavnView.UI.Surface and name it instead"
      end
    end
  end

  describe "the run spine" do
    # Both ways a caller reaches it: a HEEx tag, and a design-system example's attribute
    # map. The map form is why the first version of this test missed one — a shipped
    # example was still passing the surface, and the browser audit could not see it
    # because a doubled surface renders without failing any measurement.
    test "carries its own surface, radius, and padding, so no caller passes them" do
      owned = ~r/favn-surface|rounded-box|\bp-\d/

      for file <- styled_files(), path = Path.relative_to_cwd(file) do
        content = File.read!(file)

        for [tag] <- Regex.scan(~r/<\.run_timeline\b[^>]*>/s, content) do
          refute tag =~ owned, "#{path} styles run_timeline's own container: #{tag}"
        end

        for [_, attrs] <- Regex.scan(~r/"data\/run_timeline" => \[(.*?)\n      \]/s, content),
            [_, class] <- Regex.scan(~r/class:\s*"([^"]*)"/, attrs) do
          refute class =~ owned,
                 "#{path} gives a run_timeline example the element's own styling: #{class}"
        end
      end
    end
  end

  # Everything that renders markup, including the design-system examples — a shipped
  # example that copies a step is the same defect with a worse blast radius, because it
  # is what the next page gets written from.
  defp styled_files do
    ["lib/favn_view/**/*.{ex,heex}", "dev/**/*.{ex,heex}"]
    |> Enum.flat_map(&Path.wildcard(Path.join(@view_root, &1)))
    |> Enum.reject(&String.ends_with?(&1, "ui/typography.ex"))
  end
end
