defmodule Favn.Log do
  @moduledoc """
  Public helpers for constructing Favn log entries from user code.

  The current execution context does not emit logs yet, so these helpers return
  normalized `Favn.Log.Entry` structs for future runners/orchestrators to emit.
  """

  alias Favn.Log.Entry
  alias Favn.Run.Context

  @doc """
  Builds a debug log entry.
  """
  @spec debug(Context.t() | map() | keyword(), String.t(), map()) :: Entry.t()
  def debug(context_or_attrs, message, metadata \\ %{}),
    do: build(:debug, context_or_attrs, message, metadata)

  @doc """
  Builds an info log entry.
  """
  @spec info(Context.t() | map() | keyword(), String.t(), map()) :: Entry.t()
  def info(context_or_attrs, message, metadata \\ %{}),
    do: build(:info, context_or_attrs, message, metadata)

  @doc """
  Builds a warning log entry.
  """
  @spec warning(Context.t() | map() | keyword(), String.t(), map()) :: Entry.t()
  def warning(context_or_attrs, message, metadata \\ %{}),
    do: build(:warning, context_or_attrs, message, metadata)

  @doc """
  Builds an error log entry.
  """
  @spec error(Context.t() | map() | keyword(), String.t(), map()) :: Entry.t()
  def error(context_or_attrs, message, metadata \\ %{}),
    do: build(:error, context_or_attrs, message, metadata)

  @spec build(Entry.level(), Context.t() | map() | keyword(), String.t(), map()) :: Entry.t()
  defp build(level, %Context{} = context, message, metadata) do
    Entry.normalize(%{
      run_id: context.run_id,
      asset_ref: context.asset.ref,
      attempt: context.attempt,
      occurred_at: DateTime.utc_now(),
      level: level,
      source: :user_code,
      stream: :system,
      message: message,
      metadata: metadata
    })
  end

  defp build(level, context_or_attrs, message, metadata) do
    attrs =
      context_or_attrs
      |> Map.new()
      |> Map.merge(%{
        level: level,
        occurred_at: context_or_attrs[:occurred_at] || DateTime.utc_now(),
        message: message,
        metadata: metadata
      })

    Entry.normalize(attrs)
  end
end
