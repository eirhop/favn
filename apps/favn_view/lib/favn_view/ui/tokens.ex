defmodule FavnView.UI.Tokens do
  @moduledoc """
  Semantic tone tokens shared by every Favn UI element.

  A *tone* is the single vocabulary the design system uses to express meaning
  through colour. Components never accept raw palette classes; they accept a
  tone and ask this module for the DaisyUI class that renders it.

  ## Tones

    * `:neutral` — no judgement: counts, ids, inert metadata
    * `:primary` — the operator's current focus or selection
    * `:info` — informational, in-progress, or advisory
    * `:success` — healthy, complete, satisfied
    * `:warning` — degraded, stale, needs attention but not broken
    * `:error` — failed, blocked, or unsafe

  ## Examples

      iex> FavnView.UI.Tokens.badge_class(:success)
      "badge-success"

      iex> FavnView.UI.Tokens.tone(:failed)
      :error

      iex> FavnView.UI.Tokens.tone("running")
      :info
  """

  @type tone :: :neutral | :primary | :info | :success | :warning | :error

  @tones [:neutral, :primary, :info, :success, :warning, :error]

  @doc """
  The supported tones, in escalation order.
  """
  @spec tones() :: [tone()]
  def tones, do: @tones

  @doc """
  Normalises a domain status into a tone.

  Unknown values fall back to `:neutral` so a new backend status can never crash
  a page. Domain modules should still map their own statuses explicitly when the
  meaning is not obvious from the name.
  """
  @spec tone(atom() | String.t() | nil) :: tone()
  def tone(value) when value in @tones, do: value
  def tone(value) when is_binary(value), do: value |> normalize_string() |> tone()

  def tone(:ok), do: :success
  def tone(:healthy), do: :success
  def tone(:complete), do: :success
  def tone(:completed), do: :success
  def tone(:succeeded), do: :success
  def tone(:ready), do: :success
  def tone(:active), do: :success
  def tone(:fresh), do: :info
  def tone(:running), do: :info
  def tone(:pending), do: :info
  def tone(:queued), do: :info
  def tone(:planned), do: :info
  def tone(:stale), do: :warning
  def tone(:missed), do: :warning
  def tone(:incomplete), do: :warning
  def tone(:degraded), do: :warning
  def tone(:cancelled), do: :warning
  def tone(:canceled), do: :warning
  def tone(:failed), do: :error
  def tone(:failure), do: :error
  def tone(:blocked), do: :error
  def tone(:unknown), do: :neutral
  def tone(:uninitialized), do: :neutral
  def tone(:muted), do: :neutral
  def tone(_other), do: :neutral

  @doc """
  Badge colour class for a tone.
  """
  @spec badge_class(tone()) :: String.t()
  def badge_class(:neutral), do: "favn-badge-neutral"
  def badge_class(tone), do: "badge-" <> color(tone)

  @doc """
  Status dot colour class for a tone.
  """
  @spec dot_class(tone()) :: String.t()
  def dot_class(:neutral), do: "favn-status-neutral"
  def dot_class(tone), do: "status-" <> color(tone)

  @doc """
  Text colour class for a tone.
  """
  @spec text_class(tone()) :: String.t()
  def text_class(:neutral), do: "favn-text-muted"
  def text_class(tone), do: "text-" <> color(tone)

  @doc """
  Border colour class for a tone, at the design-system's standard opacity.
  """
  @spec border_class(tone()) :: String.t()
  def border_class(:neutral), do: "border-base-content/15"
  def border_class(tone), do: "border-" <> color(tone) <> "/35"

  @doc """
  Soft background wash for a tone, used behind icons and inline callouts.
  """
  @spec surface_class(tone()) :: String.t()
  def surface_class(:neutral), do: "bg-base-content/5"
  def surface_class(tone), do: "bg-" <> color(tone) <> "/10"

  @doc """
  Solid fill class for a tone, for bars and meters that carry meaning by area.

  Washes (`surface_class/1`) are for backgrounds behind text. A meter segment is
  the mark itself, so it needs full saturation to stay legible at a few pixels
  wide.

      iex> FavnView.UI.Tokens.fill_class(:success)
      "bg-success"

      iex> FavnView.UI.Tokens.fill_class(:neutral)
      "bg-base-content/25"
  """
  @spec fill_class(tone()) :: String.t()
  def fill_class(:neutral), do: "bg-base-content/25"
  def fill_class(tone), do: "bg-" <> color(tone)

  @doc """
  Alert colour class for a tone.

  DaisyUI has no `alert-neutral`, so the neutral tone renders as a plain
  base-content alert.
  """
  @spec alert_class(tone()) :: String.t()
  def alert_class(:neutral), do: "favn-alert-neutral"
  def alert_class(tone), do: "alert-" <> color(tone)

  @doc """
  DaisyUI button colour class for a tone.
  """
  @spec button_class(tone()) :: String.t()
  def button_class(:neutral), do: "btn-neutral"
  def button_class(tone), do: "btn-" <> color(tone)

  # `:neutral` never reaches here: every family above renders it from base tokens,
  # because DaisyUI's `neutral` colour is too dark to read on the dark theme.
  defp color(:primary), do: "primary"
  defp color(:info), do: "info"
  defp color(:success), do: "success"
  defp color(:warning), do: "warning"
  defp color(:error), do: "error"

  defp normalize_string(value) do
    value
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "_")
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :unknown
  end
end
