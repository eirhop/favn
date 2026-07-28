defmodule FavnView.Dev.DesignSystem.Palette do
  @moduledoc """
  The contrast the themes must satisfy, checked without a browser.

  ## Why a second gate

  `FavnView.Dev.DesignSystem.Audit` measures what a browser rendered, which is
  the only way to judge layout, clipping, and composited opacity. It needs a
  browser, so it cannot run in `mix test`.

  Most palette failures, though, are not layout failures: they are a token pair
  that was never going to work. The primary button's label measured 2.78:1
  because near-white text on a mid-blue fill cannot pass, and every light-theme
  status tone failed because tones tuned as fills were being read as text. Both
  are decidable from the tokens alone.

  So this gate checks pairs, in Elixir, with the same thresholds
  `Audit` uses. It would have caught both of those regressions before a browser
  was ever opened. What it deliberately does not cover — because it cannot —
  is anything depending on layout or on the actual rendered stack of translucent
  surfaces. `Audit` remains the authority there.

  ## Backgrounds

  A check's background is either a token or a wash: `{:wash, token, weight}`
  means `color-mix(in oklab, <token> <weight>, <base>)`, which is how Favn draws
  a soft badge, a notice, and an outlined button. The wash is the honest
  background for tone *text*, because a tone drawn over its own wash has less
  contrast than the same tone over the bare base.
  """

  alias FavnView.Dev.DesignSystem.Audit
  alias FavnView.Dev.DesignSystem.Color
  alias FavnView.Dev.DesignSystem.Theme

  # Tones that are read as text over their own wash: soft badges and notices.
  @tones [:info, :success, :warning, :error]

  # Every surface that can be filled with a tone and carry a label on it.
  @solid_surfaces [:primary, :secondary, :accent, :info, :success, :warning, :error]

  # The weight Favn's soft surfaces use for a tone wash. Kept as one number so
  # the gate and the stylesheet can be reconciled by reading one line.
  @soft_wash 0.16

  @type check :: %{
          id: atom(),
          theme: String.t(),
          foreground: String.t(),
          background: String.t() | {:wash, String.t(), float()},
          limit: number(),
          why: String.t()
        }

  @type verdict :: %{
          check: atom(),
          theme: String.t(),
          ratio: float(),
          limit: number(),
          status: :pass | :fail,
          foreground: String.t(),
          background: String.t()
        }

  @doc """
  Every check, for every theme.
  """
  @spec checks() :: [check()]
  def checks do
    for theme <- Theme.themes(), check <- checks_for_theme(), do: Map.put(check, :theme, theme)
  end

  @doc """
  Judges every check against the stylesheet as it stands.

  ## Examples

      iex> verdicts = FavnView.Dev.DesignSystem.Palette.verdicts()
      iex> Enum.all?(verdicts, &is_float(&1.ratio))
      true
  """
  @spec verdicts() :: [verdict()]
  def verdicts, do: Enum.map(checks(), &verdict/1)

  @doc """
  Only the checks that fail, ready to print in a test failure.
  """
  @spec failures() :: [verdict()]
  def failures, do: Enum.filter(verdicts(), &(&1.status == :fail))

  @doc """
  A one-line description of a verdict, for test output.
  """
  @spec describe(verdict()) :: String.t()
  def describe(verdict) do
    "#{verdict.theme} #{verdict.check}: #{verdict.ratio}:1 against #{verdict.limit}:1 " <>
      "(#{verdict.foreground} on #{verdict.background})"
  end

  defp checks_for_theme do
    [
      %{
        id: :body_text,
        foreground: "--color-base-content",
        background: "--color-base-100",
        limit: 4.5,
        why: "body copy on the page background"
      },
      %{
        id: :body_text_on_panel,
        foreground: "--color-base-content",
        background: "--color-base-200",
        limit: 4.5,
        why: "body copy on a panel"
      },
      %{
        id: :body_text_on_raised,
        foreground: "--color-base-content",
        background: "--color-base-300",
        limit: 4.5,
        why: "body copy on the most raised surface"
      },
      %{
        id: :link_text,
        foreground: "--color-primary",
        background: "--color-base-100",
        limit: 4.5,
        why: "primary is link text as well as the atmosphere colour"
      },
      %{
        id: :action_label,
        foreground: "--favn-action",
        background: {:wash, "--favn-action", 0.12},
        limit: 4.5,
        why: "the primary button draws its label in the action colour over its own wash"
      },
      %{
        id: :action_label_on_base,
        foreground: "--favn-action",
        background: "--color-base-100",
        limit: 4.5,
        why: "the action colour is also used for inline emphasis"
      },
      %{
        id: :muted_text,
        foreground: "--favn-text-muted",
        background: "--color-base-100",
        limit: 4.5,
        why: "metadata and labels on the page background"
      },
      %{
        id: :muted_text_on_raised,
        foreground: "--favn-text-muted",
        background: "--color-base-300",
        limit: 4.5,
        why: "the same text on the most raised surface, which is the worst case"
      },
      %{
        id: :subtle_text,
        foreground: "--favn-text-subtle",
        background: "--color-base-100",
        limit: 4.5,
        why: "the faintest tier still has to be readable"
      },
      %{
        id: :subtle_text_on_raised,
        foreground: "--favn-text-subtle",
        background: "--color-base-300",
        limit: 4.5,
        why: "the faintest tier on the most raised surface"
      }
    ] ++ Enum.map(@tones, &wash_check/1) ++ Enum.map(@solid_surfaces, &solid_check/1)
  end

  defp wash_check(tone) do
    token = "--color-#{tone}"

    %{
      id: :"#{tone}_text_on_wash",
      foreground: token,
      background: {:wash, token, @soft_wash},
      limit: 4.5,
      why: "a soft badge and a notice draw #{tone} as text over a #{tone} wash"
    }
  end

  defp solid_check(surface) do
    token = "--color-#{surface}"

    %{
      id: :"#{surface}_solid_label",
      foreground: "#{token}-content",
      background: token,
      limit: 4.5,
      why: "a solid #{surface} surface and the label on it"
    }
  end

  defp verdict(check) do
    foreground = Theme.fetch!(check.theme, check.foreground) |> Color.parse!()
    base = Theme.fetch!(check.theme, "--color-base-100") |> Color.parse!()
    {background, background_label} = background(check, base)

    ratio = Audit.contrast_ratio(Color.to_srgb(foreground), Color.to_srgb(background))

    %{
      check: check.id,
      theme: check.theme,
      ratio: ratio,
      limit: check.limit,
      status: if(ratio >= check.limit, do: :pass, else: :fail),
      foreground: Theme.fetch!(check.theme, check.foreground),
      background: background_label
    }
  end

  defp background(%{background: {:wash, token, weight}, theme: theme}, base) do
    value = Theme.fetch!(theme, token)
    mixed = Color.mix(Color.parse!(value), base, weight)

    {mixed, "#{trunc(weight * 100)}% #{value} over the base"}
  end

  defp background(%{background: token, theme: theme}, _base) do
    value = Theme.fetch!(theme, token)

    {Color.parse!(value), value}
  end
end
