defmodule Favn.Replay.InputMode do
  @moduledoc """
  Runtime-input behavior for a new or resumed run.

  `:pinned` requires source-run pins, `:inherit` reuses available source pins
  and resolves missing nodes, and `:fresh` resolves selected inputs anew.
  """

  @type t :: :pinned | :inherit | :fresh
  @modes [:pinned, :inherit, :fresh]
  @operation_defaults [
    manual: :fresh,
    scheduled: :fresh,
    backfill_child: :fresh,
    fresh_rerun: :fresh,
    exact_replay: :pinned,
    resume_from_failure: :inherit,
    retry_remaining: :inherit
  ]

  @doc "Returns all supported input modes."
  @spec values() :: [t()]
  def values, do: @modes

  @doc "Returns the published new-run operation defaults used by user documentation."
  @spec operation_defaults() :: [{atom(), t()}]
  def operation_defaults, do: @operation_defaults

  @doc "Returns the default input mode for a public run operation."
  @spec default_for(atom()) :: t()
  def default_for(operation)
      when operation in [:manual, :scheduled, :backfill_child, :fresh_rerun],
      do: :fresh

  def default_for(:exact_replay), do: :pinned

  def default_for(operation) when operation in [:resume_from_failure, :retry_remaining],
    do: :inherit

  @doc "Normalizes an input mode."
  @spec normalize(term()) :: {:ok, t()} | {:error, :invalid_input_mode}
  def normalize(value) when value in @modes, do: {:ok, value}
  def normalize("pinned"), do: {:ok, :pinned}
  def normalize("inherit"), do: {:ok, :inherit}
  def normalize("fresh"), do: {:ok, :fresh}
  def normalize(_value), do: {:error, :invalid_input_mode}
end
