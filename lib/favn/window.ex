defmodule Favn.Window do
  @moduledoc """
  Public window constructors for assets and pipelines.

  Use this module when authoring `@window` declarations or when constructing
  anchor/runtime windows directly in tests and runtime code.

  The helpers return canonical window structs used across Elixir assets, SQL
  assets, pipelines, backfills, and freshness checks.

  ## Window types

  - `hourly/1`, `daily/1`, `monthly/1` build asset-level `%Favn.Window.Spec{}` values
  - `anchor/4` builds a run-level `%Favn.Window.Anchor{}`
  - `runtime/5` builds a concrete `%Favn.Window.Runtime{}` for one execution node

  ## Spec options

  `hourly/1`, `daily/1`, and `monthly/1` accept these keyword options:

  - `lookback`: non-negative integer, defaults to `0`
  - `refresh_from`: lower or equal-granularity refresh boundary
  - `timezone`: IANA timezone string, defaults to `"Etc/UTC"`

  Supported `refresh_from` values:

  - hourly: `nil | :hour`
  - daily: `nil | :hour | :day`
  - monthly: `nil | :day | :month`

  ## Anchor/runtime options

  `anchor/4` and `runtime/5` accept:

  - `timezone`: IANA timezone string, defaults to `"Etc/UTC"`

  `start_at` must be strictly before `end_at`.

  ## Examples

      Favn.Window.daily()

      Favn.Window.daily(lookback: 2, refresh_from: :hour, timezone: "Europe/Oslo")

      Favn.Window.monthly(refresh_from: :day)

      anchor =
        Favn.Window.anchor(
          :day,
          DateTime.from_naive!(~N[2026-04-01 00:00:00], "Etc/UTC"),
          DateTime.from_naive!(~N[2026-04-02 00:00:00], "Etc/UTC")
        )

      Favn.Window.runtime(
        :day,
        DateTime.from_naive!(~N[2026-03-31 00:00:00], "Etc/UTC"),
        DateTime.from_naive!(~N[2026-04-01 00:00:00], "Etc/UTC"),
        anchor.key
      )

  ## When to use what

  - use spec helpers in asset DSLs such as `@window Favn.Window.daily(...)`
  - use anchor windows when operators or pipelines request a run range
  - use runtime windows when testing or working with resolved execution nodes
  """

  alias Favn.Window.{Anchor, Runtime, Spec}

  @type spec_kind :: Spec.kind()

  @doc """
  Builds an hourly window spec.

  Supported options:

  - `lookback`
  - `refresh_from: :hour`
  - `timezone`

  ## Example

      Favn.Window.hourly(lookback: 6, refresh_from: :hour)
  """
  @spec hourly(keyword()) :: Spec.t()
  def hourly(opts \\ []), do: Spec.new!(:hour, opts)

  @doc """
  Builds a daily window spec.

  Supported options:

  - `lookback`
  - `refresh_from: :hour | :day`
  - `timezone`

  ## Examples

      Favn.Window.daily()
      Favn.Window.daily(lookback: 1)
      Favn.Window.daily(lookback: 7, refresh_from: :hour, timezone: "Europe/Oslo")
  """
  @spec daily(keyword()) :: Spec.t()
  def daily(opts \\ []), do: Spec.new!(:day, opts)

  @doc """
  Builds a monthly window spec.

  Supported options:

  - `lookback`
  - `refresh_from: :day | :month`
  - `timezone`

  ## Example

      Favn.Window.monthly(lookback: 1, refresh_from: :day)
  """
  @spec monthly(keyword()) :: Spec.t()
  def monthly(opts \\ []), do: Spec.new!(:month, opts)

  @doc """
  Builds a named anchor window.

  Use this for run-level execution intent such as pipeline runs and backfills.

  Supported options:

  - `timezone`

  ## Example

      Favn.Window.anchor(
        :day,
        DateTime.from_naive!(~N[2026-04-01 00:00:00], "Etc/UTC"),
        DateTime.from_naive!(~N[2026-04-02 00:00:00], "Etc/UTC"),
        timezone: "Europe/Oslo"
      )
  """
  @spec anchor(spec_kind(), DateTime.t(), DateTime.t(), keyword()) :: Anchor.t()
  def anchor(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) do
    Anchor.new!(kind, start_at, end_at, opts)
  end

  @doc """
  Builds a runtime window keyed to an anchor window.

  Use this when a planner has expanded an anchor window into concrete execution
  nodes or when tests need a fully resolved runtime window.

  Supported options:

  - `timezone`

  ## Example

      anchor =
        Favn.Window.anchor(
          :day,
          DateTime.from_naive!(~N[2026-04-01 00:00:00], "Etc/UTC"),
          DateTime.from_naive!(~N[2026-04-02 00:00:00], "Etc/UTC")
        )

      Favn.Window.runtime(
        :day,
        DateTime.from_naive!(~N[2026-03-31 00:00:00], "Etc/UTC"),
        DateTime.from_naive!(~N[2026-04-01 00:00:00], "Etc/UTC"),
        anchor.key
      )
  """
  @spec runtime(spec_kind(), DateTime.t(), DateTime.t(), Favn.Window.Key.t(), keyword()) ::
          Runtime.t()
  def runtime(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    Runtime.new!(kind, start_at, end_at, anchor_key, opts)
  end
end
