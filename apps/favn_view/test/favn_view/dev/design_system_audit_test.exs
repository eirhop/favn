defmodule FavnView.Dev.DesignSystem.AuditTest do
  use ExUnit.Case, async: true

  alias FavnView.Dev.DesignSystem.Audit

  doctest Audit

  describe "rules" do
    test "every rule names a metric, an operator, a limit, and a reason" do
      for rule <- Audit.rules() do
        assert is_atom(rule.id)
        assert is_atom(rule.metric)
        assert rule.op in [:gte, :lte]
        assert is_number(rule.limit)
        assert rule.why != ""
      end
    end

    test "rule ids are unique" do
      ids = Enum.map(Audit.rules(), & &1.id)
      assert ids == Enum.uniq(ids)
    end

    test "the payload handed to the browser is JSON-encodable and complete" do
      payload = Audit.rules_payload()

      assert length(payload) == length(Audit.rules())
      assert {:ok, _json} = Jason.encode(payload)

      for rule <- payload do
        assert Map.keys(rule) |> Enum.sort() == ~w(applies id inactive_exempt limit metric op why)
      end
    end
  end

  describe "evaluate/1" do
    test "applies only the rules that match the element's kinds" do
      checks = Audit.evaluate(%{kinds: [:control], min_side: 32})

      assert Enum.map(checks, & &1.rule) == [:hit_target]
    end

    test "an element that is both text and a control is judged on both" do
      checks = Audit.evaluate(%{kinds: [:text, :control], contrast: 7.0, min_side: 40})

      assert Enum.map(checks, & &1.rule) |> Enum.sort() == [:hit_target, :text_contrast]
      assert Enum.all?(checks, &(&1.status == :pass))
    end

    test "a value below the limit fails and reports both value and limit" do
      [check] = Audit.evaluate(%{kinds: [:control], min_side: 18})

      assert check.status == :fail
      assert check.value == 18
      assert check.limit == 24
    end

    test "large text is judged against the lower limit, not the normal one" do
      [check] = Audit.evaluate(%{kinds: [:large_text], contrast: 3.2})

      assert check.rule == :large_text_contrast
      assert check.status == :pass
    end

    test "a metric the browser could not measure is skipped, never passed" do
      [check] = Audit.evaluate(%{kinds: [:boundary]})

      assert check.status == :skipped
      assert check.reason == :not_measured
      assert check.value == nil
    end

    test "a non-numeric measurement is skipped with its own reason" do
      [check] = Audit.evaluate(%{kinds: [:text], contrast: "oklch(70% 0.1 250)"})

      assert check.status == :skipped
      assert check.reason == :not_a_number
    end

    test "clipping is judged per example and passes at zero" do
      assert [%{rule: :no_clipped_content, status: :pass}] =
               Audit.evaluate(%{kinds: [:example], clipped_px: 0})

      assert [%{rule: :no_clipped_content, status: :fail}] =
               Audit.evaluate(%{kinds: [:example], clipped_px: 14})
    end

    test "an icon-only control with no accessible name fails" do
      [check] = Audit.evaluate(%{kinds: [:icon_control], accessible_name_length: 0})

      assert check.rule == :accessible_name
      assert check.status == :fail
    end

    test "a disabled control is exempt from contrast and target size" do
      checks =
        Audit.evaluate(%{kinds: [:text, :control], inactive: true, contrast: 1.4, min_side: 12})

      assert Enum.map(checks, &{&1.rule, &1.status, &1.reason}) == [
               {:text_contrast, :skipped, :inactive_control},
               {:hit_target, :skipped, :inactive_control}
             ]
    end

    test "a disabled control still needs an accessible name" do
      [check] =
        Audit.evaluate(%{kinds: [:icon_control], inactive: true, accessible_name_length: 0})

      assert check.rule == :accessible_name
      assert check.status == :fail
    end
  end

  describe "summarize/1" do
    test "counts each status" do
      checks =
        Audit.evaluate(%{kinds: [:text, :control], contrast: 1.2, min_side: 40}) ++
          Audit.evaluate(%{kinds: [:boundary]})

      assert Audit.summarize(checks) == %{pass: 1, fail: 1, skipped: 1}
    end
  end

  describe "contrast" do
    test "is symmetric" do
      assert Audit.contrast_ratio({12, 18, 30}, {220, 226, 240}) ==
               Audit.contrast_ratio({220, 226, 240}, {12, 18, 30})
    end

    test "the dark theme's body text clears the AA threshold" do
      # oklch(12% 0.035 260) base against near-white content, as favn-dark renders.
      assert Audit.contrast_ratio({15, 18, 28}, {226, 232, 240}) >= 4.5
    end

    test "a neutral badge on a dark surface can fail, which is the bug this catches" do
      assert Audit.contrast_ratio({45, 52, 64}, {26, 30, 40}) < 4.5
    end

    test "composites a translucent foreground over its backdrop before judging" do
      faint = Audit.composite({255, 255, 255, 0.08}, {15, 18, 28})

      assert Audit.contrast_ratio(faint, {15, 18, 28}) < 1.5
    end
  end
end
