defmodule FavnView.Dev.DesignSystem.Audit do
  @moduledoc """
  The thresholds the rendered design system must satisfy, and the verdicts.

  ## Why this exists

  Colour, contrast, and target size are the things a screenshot is worst at.
  An invisible badge and a correct badge can look equally plausible at a glance,
  and a 1.4:1 contrast ratio is unmistakable as a number. So the browser
  measures, this module judges, and a screenshot is only needed for the
  questions measurement genuinely cannot answer: rhythm, balance, and whether
  the screen reads as one system.

  ## Rules are data

  A rule is `{metric, op, limit}` plus the kind of element it applies to.
  Because a rule is data, the same set drives two consumers without either one
  restating a threshold:

    * the browser script from
      `FavnView.Dev.DesignSystem.AuditScript`, which receives `rules/0` with the
      rendered page and interprets them against what it measured;
    * `evaluate/1` here, which interprets them against measurements supplied by
      a test.

  Changing 4.5 to 7.0 changes both. Neither one hard-codes a number.

  ## Verdicts are three-valued

  A check is `:pass`, `:fail`, or `:skipped`. A metric the browser could not
  measure — an unresolvable colour function, an element with no box — is
  `:skipped` with a reason, never a pass. A viewer that reports "no failures"
  when it measured nothing is worse than one that reports nothing.
  """

  @type op :: :gte | :lte
  @type kind :: :text | :large_text | :control | :icon_control | :boundary | :example

  @type rule :: %{
          id: atom(),
          metric: atom(),
          op: op(),
          limit: number(),
          applies: kind(),
          inactive_exempt: boolean(),
          why: String.t()
        }

  @type check :: %{
          rule: atom(),
          metric: atom(),
          op: op(),
          limit: number(),
          value: number() | nil,
          status: :pass | :fail | :skipped,
          reason: atom() | nil
        }

  # `inactive_exempt` encodes WCAG's inactive-component exception: contrast and
  # target size do not apply to a disabled control, but an accessible name and
  # unclipped content still do — a disabled control is still perceived.
  @rules [
    %{
      id: :text_contrast,
      metric: :contrast,
      op: :gte,
      limit: 4.5,
      applies: :text,
      inactive_exempt: true,
      why: "WCAG 2.2 AA, normal text against its own background"
    },
    %{
      id: :large_text_contrast,
      metric: :contrast,
      op: :gte,
      limit: 3.0,
      applies: :large_text,
      inactive_exempt: true,
      why: "WCAG 2.2 AA, text at 24px or 18.66px bold and above"
    },
    %{
      id: :boundary_contrast,
      metric: :boundary_contrast,
      op: :gte,
      limit: 3.0,
      applies: :boundary,
      inactive_exempt: true,
      why: "WCAG 2.2 AA non-text contrast: a border that carries meaning must be visible"
    },
    %{
      id: :hit_target,
      metric: :min_side,
      op: :gte,
      limit: 24,
      applies: :control,
      inactive_exempt: true,
      why: "WCAG 2.2 AA target size (minimum)"
    },
    %{
      id: :accessible_name,
      metric: :accessible_name_length,
      op: :gte,
      limit: 1,
      applies: :icon_control,
      inactive_exempt: false,
      why: "an icon-only control has no visible label, so it needs an accessible name"
    },
    %{
      id: :no_clipped_content,
      metric: :clipped_px,
      op: :lte,
      limit: 0,
      applies: :example,
      inactive_exempt: false,
      why: "content cut off without an ellipsis is a layout failure, not truncation"
    }
  ]

  @doc """
  Every rule, in report order.
  """
  @spec rules() :: [rule()]
  def rules, do: @rules

  @doc """
  The rules as JSON-encodable maps, for handing to the browser.
  """
  @spec rules_payload() :: [map()]
  def rules_payload do
    Enum.map(@rules, fn rule ->
      %{
        "id" => Atom.to_string(rule.id),
        "metric" => Atom.to_string(rule.metric),
        "op" => Atom.to_string(rule.op),
        "limit" => rule.limit,
        "applies" => Atom.to_string(rule.applies),
        "inactive_exempt" => rule.inactive_exempt,
        "why" => rule.why
      }
    end)
  end

  @doc """
  Judges one measurement against every rule that applies to it.

  A measurement is a map of metric values plus `:kinds`, the list of element
  kinds it counts as. An element is often several kinds at once: a button with a
  label is both `:text` and `:control`.

  ## Examples

      iex> alias FavnView.Dev.DesignSystem.Audit
      iex> Audit.evaluate(%{kinds: [:text], contrast: 6.1})
      [%{rule: :text_contrast, metric: :contrast, op: :gte, limit: 4.5, value: 6.1, status: :pass, reason: nil}]

      iex> alias FavnView.Dev.DesignSystem.Audit
      iex> [check] = Audit.evaluate(%{kinds: [:text], contrast: 1.4})
      iex> check.status
      :fail

      iex> alias FavnView.Dev.DesignSystem.Audit
      iex> [check] = Audit.evaluate(%{kinds: [:text]})
      iex> {check.status, check.reason}
      {:skipped, :not_measured}

  A disabled control is exempt from contrast and target size, but not from
  needing an accessible name:

      iex> alias FavnView.Dev.DesignSystem.Audit
      iex> checks = Audit.evaluate(%{kinds: [:text, :icon_control], inactive: true, contrast: 1.4, accessible_name_length: 0})
      iex> Enum.map(checks, &{&1.rule, &1.status, &1.reason})
      [{:text_contrast, :skipped, :inactive_control}, {:accessible_name, :fail, nil}]
  """
  @spec evaluate(map()) :: [check()]
  def evaluate(measurement) when is_map(measurement) do
    kinds = Map.get(measurement, :kinds, [])

    for rule <- @rules, rule.applies in kinds, do: check(rule, measurement)
  end

  @doc """
  Counts pass, fail, and skipped across many checks.
  """
  @spec summarize([check()]) :: %{
          pass: non_neg_integer(),
          fail: non_neg_integer(),
          skipped: non_neg_integer()
        }
  def summarize(checks) do
    Enum.reduce(checks, %{pass: 0, fail: 0, skipped: 0}, fn check, acc ->
      Map.update!(acc, check.status, &(&1 + 1))
    end)
  end

  @doc """
  The WCAG relative-luminance contrast ratio between two opaque sRGB colours.

  Channels are 0..255.

  ## Examples

      iex> FavnView.Dev.DesignSystem.Audit.contrast_ratio({0, 0, 0}, {255, 255, 255})
      21.0

      iex> FavnView.Dev.DesignSystem.Audit.contrast_ratio({255, 255, 255}, {255, 255, 255})
      1.0
  """
  @spec contrast_ratio({number(), number(), number()}, {number(), number(), number()}) :: float()
  def contrast_ratio(first, second) do
    lighter = max(relative_luminance(first), relative_luminance(second))
    darker = min(relative_luminance(first), relative_luminance(second))

    Float.round((lighter + 0.05) / (darker + 0.05), 2)
  end

  @doc """
  WCAG relative luminance of an opaque sRGB colour, channels 0..255.
  """
  @spec relative_luminance({number(), number(), number()}) :: float()
  def relative_luminance({red, green, blue}) do
    0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue)
  end

  @doc """
  Composites a translucent colour over an opaque one.

  ## Examples

      iex> FavnView.Dev.DesignSystem.Audit.composite({255, 255, 255, 0.5}, {0, 0, 0})
      {128, 128, 128}
  """
  @spec composite({number(), number(), number(), number()}, {number(), number(), number()}) ::
          {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  def composite({red, green, blue, alpha}, {backdrop_red, backdrop_green, backdrop_blue}) do
    {
      blend(red, backdrop_red, alpha),
      blend(green, backdrop_green, alpha),
      blend(blue, backdrop_blue, alpha)
    }
  end

  defp blend(over, under, alpha), do: round(over * alpha + under * (1 - alpha))

  defp linear(channel) do
    ratio = channel / 255

    if ratio <= 0.03928 do
      ratio / 12.92
    else
      :math.pow((ratio + 0.055) / 1.055, 2.4)
    end
  end

  defp check(rule, measurement) do
    base = %{
      rule: rule.id,
      metric: rule.metric,
      op: rule.op,
      limit: rule.limit,
      value: nil,
      status: :skipped,
      reason: :not_measured
    }

    if Map.get(measurement, :inactive, false) and rule.inactive_exempt do
      %{base | reason: :inactive_control}
    else
      case Map.get(measurement, rule.metric) do
        nil -> base
        value when not is_number(value) -> %{base | reason: :not_a_number}
        value -> %{base | value: value, status: verdict(rule.op, value, rule.limit), reason: nil}
      end
    end
  end

  defp verdict(:gte, value, limit) when value >= limit, do: :pass
  defp verdict(:lte, value, limit) when value <= limit, do: :pass
  defp verdict(_op, _value, _limit), do: :fail
end
