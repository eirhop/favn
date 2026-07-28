defmodule FavnView.Dev.DesignSystem.PaletteTest do
  use ExUnit.Case, async: true

  alias FavnView.Dev.DesignSystem.Color
  alias FavnView.Dev.DesignSystem.Palette
  alias FavnView.Dev.DesignSystem.Theme

  doctest Color
  doctest Theme
  doctest Palette

  describe "the palette" do
    test "every token pair the themes rely on clears its contrast limit" do
      failures = Palette.failures()

      assert failures == [],
             """
             The palette does not meet its own contrast limits.

             #{Enum.map_join(failures, "\n", &("  " <> Palette.describe(&1)))}

             These are token pairs, not layout: fix the token in
             apps/favn_view/assets/css/app.css rather than the call site.
             """
    end

    test "both themes are checked, not just the default one" do
      themes = Palette.verdicts() |> Enum.map(& &1.theme) |> Enum.uniq() |> Enum.sort()

      assert themes == ["favn-dark", "favn-light"]
    end

    test "a check names why it exists" do
      for check <- Palette.checks() do
        assert check.why != ""
        assert is_atom(check.id)
      end
    end
  end

  describe "colour conversion" do
    test "agrees with the browser on an oklab mix" do
      # Chrome resolves
      #   color-mix(in oklab, oklch(87% 0.23 130) 12%, oklch(12% 0.035 260))
      # to oklab(0.21 -0.0230893 -0.00918926). If this drifts, every verdict in
      # the gate is measuring something the browser does not render.
      mixed =
        Color.mix(
          Color.parse!("oklch(87% 0.23 130)"),
          Color.parse!("oklch(12% 0.035 260)"),
          0.12
        )

      assert Float.round(mixed.l, 4) == 0.21
      assert Float.round(mixed.a, 7) == -0.0230893
      assert Float.round(mixed.b, 7) == -0.0091893
    end

    test "round-trips sRGB through oklab" do
      for hex <- ~w(#000000 #ffffff #ff0000 #1e293b #7fff00) do
        {:ok, color} = Color.parse(hex)
        {red, green, blue} = Color.to_srgb(color)
        {:ok, reparsed} = Color.parse("rgb(#{red}, #{green}, #{blue})")

        assert Color.to_srgb(reparsed) == {red, green, blue}
      end
    end

    test "reproduces the contrast the browser measured for the primary button" do
      # The browser measured 12.52:1 for the action label over its own wash.
      action = Color.parse!(Theme.fetch!("favn-dark", "--favn-action"))
      base = Color.parse!(Theme.fetch!("favn-dark", "--color-base-100"))
      wash = Color.mix(action, base, 0.12)

      ratio =
        FavnView.Dev.DesignSystem.Audit.contrast_ratio(
          Color.to_srgb(action),
          Color.to_srgb(wash)
        )

      assert_in_delta ratio, 12.52, 0.15
    end

    test "refuses a value it cannot convert instead of guessing" do
      assert Color.parse("linear-gradient(red, blue)") == {:error, :unsupported}
      assert Color.parse("currentColor") == {:error, :unsupported}
      assert_raise ArgumentError, fn -> Color.parse!("var(--nope)") end
    end
  end

  describe "theme tokens" do
    test "are read from the stylesheet rather than restated" do
      dark = Theme.tokens("favn-dark")
      light = Theme.tokens("favn-light")

      assert dark["--color-base-100"] != light["--color-base-100"]
      assert dark["--favn-action"] != light["--favn-action"]
      assert Map.has_key?(dark, "--color-success")
      assert Map.has_key?(dark, "--radius-box")
    end

    test "a missing token names the file to fix" do
      error = assert_raise KeyError, fn -> Theme.fetch!("favn-dark", "--color-nope") end

      assert error.message =~ "app.css"
    end
  end
end
