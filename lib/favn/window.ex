defmodule Favn.Window do
  @moduledoc """
  Public runtime windowing primitives.

  This module provides small constructor helpers for canonical window structs.
  """

  alias Favn.Window.{Anchor, Runtime, Spec}

  @type spec_kind :: Spec.kind()

  @spec hourly(keyword()) :: Spec.t()
  def hourly(opts \\ []), do: Spec.new!(:hour, opts)

  @spec daily(keyword()) :: Spec.t()
  def daily(opts \\ []), do: Spec.new!(:day, opts)

  @spec monthly(keyword()) :: Spec.t()
  def monthly(opts \\ []), do: Spec.new!(:month, opts)

  @spec anchor(spec_kind(), DateTime.t(), DateTime.t(), keyword()) :: Anchor.t()
  def anchor(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) do
    Anchor.new!(kind, start_at, end_at, opts)
  end

  @spec runtime(spec_kind(), DateTime.t(), DateTime.t(), Favn.Window.Key.t(), keyword()) ::
          Runtime.t()
  def runtime(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    Runtime.new!(kind, start_at, end_at, anchor_key, opts)
  end
end
