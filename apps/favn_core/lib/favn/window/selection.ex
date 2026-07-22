defmodule Favn.Window.Selection do
  @moduledoc """
  Deterministic pipeline anchor selection.

  A selection preserves the anchors explicitly requested by a trigger, the
  permitted expansion, and the final anchors supplied to asset planning.
  Scheduled selections may apply pipeline lookback. Manual and backfill
  selections are always exact.
  """

  alias Favn.TimePeriod
  alias Favn.Window.{Anchor, Key, Validate}

  @type intent :: :scheduled | :manual | :backfill
  @type expansion :: :none | {:lookback, non_neg_integer()}

  @type t :: %__MODULE__{
          intent: intent(),
          requested_anchors: [Anchor.t()],
          expansion: expansion(),
          effective_anchors: [Anchor.t()],
          timezone: String.t()
        }

  @enforce_keys [:intent, :requested_anchors, :expansion, :effective_anchors, :timezone]
  defstruct [:intent, :requested_anchors, :expansion, :effective_anchors, :timezone]

  @doc "Builds a scheduled selection and applies the pipeline lookback once."
  @spec scheduled(Anchor.t(), non_neg_integer(), String.t()) ::
          {:ok, t()} | {:error, term()}
  def scheduled(%Anchor{} = anchor, lookback, timezone) do
    new(:scheduled, [anchor], {:lookback, lookback}, timezone)
  end

  @doc "Builds an exact manual selection."
  @spec manual(Anchor.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def manual(%Anchor{} = anchor, timezone), do: new(:manual, [anchor], :none, timezone)

  @doc "Builds an exact backfill selection."
  @spec backfill([Anchor.t()], String.t()) :: {:ok, t()} | {:error, term()}
  def backfill(anchors, timezone) when is_list(anchors),
    do: new(:backfill, anchors, :none, timezone)

  @doc "Builds and validates a canonical selection."
  @spec new(intent(), [Anchor.t()], expansion(), String.t()) ::
          {:ok, t()} | {:error, term()}
  def new(intent, requested, expansion, timezone) do
    with :ok <- validate_intent(intent),
         :ok <- Validate.timezone(timezone),
         {:ok, requested} <- normalize_anchors(requested, timezone),
         :ok <- validate_requested(intent, requested),
         :ok <- validate_anchor_kinds(requested),
         :ok <- validate_expansion(intent, expansion),
         {:ok, effective} <- expand(requested, expansion, timezone) do
      {:ok,
       %__MODULE__{
         intent: intent,
         requested_anchors: requested,
         expansion: expansion,
         effective_anchors: effective,
         timezone: timezone
       }}
    end
  end

  @doc "Rehydrates and validates a manifest-shaped selection."
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}

  def from_value(%__MODULE__{} = selection) do
    with {:ok, rebuilt} <-
           new(
             selection.intent,
             selection.requested_anchors,
             selection.expansion,
             selection.timezone
           ),
         true <- rebuilt.effective_anchors == selection.effective_anchors do
      {:ok, selection}
    else
      false -> {:error, :selection_effective_anchors_mismatch}
      {:error, _reason} = error -> error
    end
  end

  def from_value(value) when is_map(value) do
    with {:ok, intent} <- decode_intent(field(value, :intent)),
         {:ok, expansion} <- decode_expansion(field(value, :expansion)),
         {:ok, requested} <- decode_anchors(field(value, :requested_anchors, [])),
         {:ok, effective} <- decode_anchors(field(value, :effective_anchors, [])),
         {:ok, selection} <- new(intent, requested, expansion, field(value, :timezone)),
         true <- selection.effective_anchors == effective do
      {:ok, selection}
    else
      false -> {:error, :selection_effective_anchors_mismatch}
      {:error, _reason} = error -> error
    end
  end

  def from_value(value), do: {:error, {:invalid_window_selection, value}}

  defp expand(requested, :none, _timezone), do: {:ok, requested}

  defp expand([%Anchor{} = requested], {:lookback, lookback}, timezone) do
    first_start = TimePeriod.shift!(requested.start_at, requested.kind, -lookback)

    anchors =
      for offset <- 0..lookback do
        start_at = TimePeriod.shift!(first_start, requested.kind, offset)
        end_at = TimePeriod.shift!(start_at, requested.kind, 1)
        Anchor.new!(requested.kind, start_at, end_at, timezone: timezone)
      end

    {:ok, anchors}
  end

  defp normalize_anchors(values, timezone) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case Anchor.from_value(value) do
        {:ok, %Anchor{timezone: ^timezone} = anchor} -> {:cont, {:ok, [anchor | acc]}}
        {:ok, %Anchor{timezone: other}} -> {:halt, {:error, {:anchor_timezone_mismatch, other}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, anchors} ->
        {:ok,
         anchors
         |> Enum.reverse()
         |> Enum.uniq_by(&Key.encode(&1.key))
         |> Enum.sort_by(&Key.encode(&1.key))}

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_anchors(value, _timezone), do: {:error, {:invalid_selection_anchors, value}}

  defp validate_requested(_intent, []), do: {:error, :empty_window_selection}
  defp validate_requested(:scheduled, [_one]), do: :ok

  defp validate_requested(:scheduled, _many),
    do: {:error, :scheduled_selection_requires_one_anchor}

  defp validate_requested(_intent, _anchors), do: :ok

  defp validate_anchor_kinds([%Anchor{kind: kind} | rest]) do
    if Enum.all?(rest, &(&1.kind == kind)) do
      :ok
    else
      {:error, :mixed_selection_anchor_kinds}
    end
  end

  defp validate_intent(intent) when intent in [:scheduled, :manual, :backfill], do: :ok
  defp validate_intent(intent), do: {:error, {:invalid_selection_intent, intent}}

  defp validate_expansion(:scheduled, {:lookback, value})
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_expansion(intent, :none) when intent in [:manual, :backfill], do: :ok

  defp validate_expansion(intent, expansion),
    do: {:error, {:invalid_selection_expansion, intent, expansion}}

  defp decode_anchors(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case Anchor.from_value(value) do
        {:ok, %Anchor{} = anchor} -> {:cont, {:ok, [anchor | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, anchors} -> {:ok, Enum.reverse(anchors)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_anchors(value), do: {:error, {:invalid_selection_anchors, value}}

  defp decode_intent(value) when value in [:scheduled, :manual, :backfill], do: {:ok, value}
  defp decode_intent("scheduled"), do: {:ok, :scheduled}
  defp decode_intent("manual"), do: {:ok, :manual}
  defp decode_intent("backfill"), do: {:ok, :backfill}
  defp decode_intent(value), do: {:error, {:invalid_selection_intent, value}}

  defp decode_expansion(:none), do: {:ok, :none}
  defp decode_expansion("none"), do: {:ok, :none}
  defp decode_expansion({:lookback, value}), do: {:ok, {:lookback, value}}
  defp decode_expansion(["lookback", value]), do: {:ok, {:lookback, value}}
  defp decode_expansion([:lookback, value]), do: {:ok, {:lookback, value}}
  defp decode_expansion(value), do: {:error, {:invalid_selection_expansion, value}}

  defp field(value, key, default \\ nil),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
end
