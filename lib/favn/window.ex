defmodule Favn.Window do
  @moduledoc """
  Public window constructors for assets and pipelines.

  Use this module when authoring `@window` declarations or when constructing
  anchor/runtime windows directly in tests and runtime code.

  The helpers return canonical window structs used across Elixir assets, SQL
  assets, pipelines, backfills, and freshness checks.
  """

  alias Favn.Window.{Anchor, Runtime, Spec}

  @type spec_kind :: Spec.kind()

  @doc """
  Builds an hourly window spec.
  """
  @spec hourly(keyword()) :: Spec.t()
  def hourly(opts \\ []), do: Spec.new!(:hour, opts)

  @doc """
  Builds a daily window spec.
  """
  @spec daily(keyword()) :: Spec.t()
  def daily(opts \\ []), do: Spec.new!(:day, opts)

  @doc """
  Builds a monthly window spec.
  """
  @spec monthly(keyword()) :: Spec.t()
  def monthly(opts \\ []), do: Spec.new!(:month, opts)

  @doc """
  Builds a named anchor window.
  """
  @spec anchor(spec_kind(), DateTime.t(), DateTime.t(), keyword()) :: Anchor.t()
  def anchor(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) do
    Anchor.new!(kind, start_at, end_at, opts)
  end

  @doc """
  Builds a runtime window keyed to an anchor window.
  """
  @spec runtime(spec_kind(), DateTime.t(), DateTime.t(), Favn.Window.Key.t(), keyword()) ::
          Runtime.t()
  def runtime(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    Runtime.new!(kind, start_at, end_at, anchor_key, opts)
  end
end
