defmodule Favn.Backfill.RangeRequest do
  @moduledoc """
  Operator request for resolving an operational backfill range.

  The request supports two modes:

  - explicit ranges with `from`, `to`, `kind`, and `timezone`
  - relative ranges with `last: {count, kind}` and either `relative_to` or a
    baseline map containing `coverage_until`

  Explicit `to` values are inclusive at the request level. The resolver expands
  them to complete anchor windows through that period's end boundary.

  Use this module for operator input before calling
  `Favn.Backfill.RangeResolver.resolve/1`. It does not submit runs or touch
  orchestrator state.
  """

  alias Favn.Window.{Policy, Validate}

  @type kind :: Validate.kind()
  @type baseline ::
          %{optional(:coverage_until) => DateTime.t()} | %{optional(String.t()) => DateTime.t()}

  @type t :: %__MODULE__{
          mode: :explicit | :relative_last,
          kind: kind(),
          timezone: String.t(),
          from: String.t() | nil,
          to: String.t() | nil,
          last: {pos_integer(), kind()} | nil,
          relative_to: DateTime.t() | nil,
          baseline: baseline() | nil
        }

  defstruct [
    :from,
    :to,
    :last,
    :relative_to,
    :baseline,
    :kind,
    mode: :explicit,
    timezone: "Etc/UTC"
  ]

  @doc """
  Builds an explicit backfill range request.

  Required options are `:from`, `:to`, and `:kind`. `:timezone` defaults to
  `"Etc/UTC"`.

  ## Example

      {:ok, request} =
        Favn.Backfill.RangeRequest.explicit(
          from: "2026-01-01",
          to: "2026-01-07",
          kind: :day,
          timezone: "Etc/UTC"
        )
  """
  @spec explicit(keyword()) :: {:ok, t()} | {:error, term()}
  def explicit(opts) when is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:from, :to, :kind, :timezone]),
         {:ok, kind} <- normalize_kind(Keyword.get(opts, :kind)),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- Validate.timezone(timezone),
         from when is_binary(from) <- Keyword.get(opts, :from),
         to when is_binary(to) <- Keyword.get(opts, :to) do
      {:ok, %__MODULE__{mode: :explicit, kind: kind, timezone: timezone, from: from, to: to}}
    else
      nil -> {:error, {:invalid_backfill_range_request, opts}}
      false -> {:error, {:invalid_backfill_range_request, opts}}
      {:error, _reason} = error -> error
      _other -> {:error, {:invalid_backfill_range_request, opts}}
    end
  end

  @doc """
  Builds a relative-last backfill range request.

  Required input is `last: {count, kind}` plus either `:relative_to` or a
  `:baseline` map containing `:coverage_until`.

  Relative requests are useful after a baseline/cutover run has established
  coverage. The resolver expands the last complete windows before that
  reference.

  ## Example

      {:ok, request} =
        Favn.Backfill.RangeRequest.relative_last(
          last: {7, :day},
          baseline: %{coverage_until: ~U[2026-02-01 00:00:00Z]}
        )
  """
  @spec relative_last(keyword()) :: {:ok, t()} | {:error, term()}
  def relative_last(opts) when is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:last, :timezone, :relative_to, :baseline]),
         {:ok, count, kind} <- normalize_last(Keyword.get(opts, :last)),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- Validate.timezone(timezone),
         {:ok, relative_to, baseline} <- normalize_reference(opts) do
      {:ok,
       %__MODULE__{
         mode: :relative_last,
         kind: kind,
         timezone: timezone,
         last: {count, kind},
         relative_to: relative_to,
         baseline: baseline
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Coerces a struct, keyword list, or map into a validated range request.
  """
  @spec from_value(t() | map() | keyword()) :: {:ok, t()} | {:error, term()}
  def from_value(%__MODULE__{} = request), do: validate(request)
  def from_value(opts) when is_list(opts), do: opts |> Map.new() |> from_value()

  def from_value(value) when is_map(value) do
    cond do
      present?(value, :last) ->
        value |> to_keyword([:last, :timezone, :relative_to, :baseline]) |> relative_last()

      present?(value, :from) and present?(value, :to) ->
        value |> to_keyword([:from, :to, :kind, :timezone]) |> explicit()

      true ->
        {:error, {:invalid_backfill_range_request, value}}
    end
  end

  def from_value(value), do: {:error, {:invalid_backfill_range_request, value}}

  @doc """
  Validates a range request struct without resolving it.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(
        %__MODULE__{mode: :explicit, kind: kind, timezone: timezone, from: from, to: to} = request
      )
      when is_binary(from) and is_binary(to) do
    with :ok <- Validate.kind(kind),
         :ok <- Validate.timezone(timezone) do
      {:ok, request}
    end
  end

  def validate(
        %__MODULE__{mode: :relative_last, kind: kind, timezone: timezone, last: {count, kind}} =
          request
      )
      when is_integer(count) and count > 0 do
    with :ok <- Validate.kind(kind),
         :ok <- Validate.timezone(timezone),
         {:ok, _reference, _baseline} <- normalize_reference(relative_opts(request)) do
      {:ok, request}
    end
  end

  def validate(request), do: {:error, {:invalid_backfill_range_request, request}}

  defp normalize_last({count, kind}) when is_integer(count) and count > 0 do
    with {:ok, kind} <- normalize_kind(kind), do: {:ok, count, kind}
  end

  defp normalize_last([count, kind]) when is_integer(count) and count > 0 do
    with {:ok, kind} <- normalize_kind(kind), do: {:ok, count, kind}
  end

  defp normalize_last(%{count: count, kind: kind}) when is_integer(count) and count > 0 do
    with {:ok, kind} <- normalize_kind(kind), do: {:ok, count, kind}
  end

  defp normalize_last(%{"count" => count, "kind" => kind}) when is_integer(count) and count > 0 do
    with {:ok, kind} <- normalize_kind(kind), do: {:ok, count, kind}
  end

  defp normalize_last(value), do: {:error, {:invalid_last_request, value}}

  defp normalize_kind(kind) when is_atom(kind), do: Policy.normalize_kind(kind)

  defp normalize_kind("hour"), do: {:ok, :hour}
  defp normalize_kind("hourly"), do: {:ok, :hour}
  defp normalize_kind("day"), do: {:ok, :day}
  defp normalize_kind("daily"), do: {:ok, :day}
  defp normalize_kind("month"), do: {:ok, :month}
  defp normalize_kind("monthly"), do: {:ok, :month}
  defp normalize_kind("year"), do: {:ok, :year}
  defp normalize_kind("yearly"), do: {:ok, :year}

  defp normalize_kind(kind) when is_binary(kind),
    do: {:error, {:invalid_window_policy_kind, kind}}

  defp normalize_kind(kind), do: {:error, {:invalid_window_policy_kind, kind}}

  defp normalize_reference(opts) do
    relative_to = Keyword.get(opts, :relative_to)
    baseline = Keyword.get(opts, :baseline)

    cond do
      match?(%DateTime{}, parse_datetime(relative_to)) ->
        {:ok, parse_datetime(relative_to), baseline}

      match?(%DateTime{}, parse_datetime(coverage_until(baseline))) ->
        {:ok, parse_datetime(coverage_until(baseline)), normalize_baseline(baseline)}

      true ->
        {:error, {:missing_backfill_reference, opts}}
    end
  end

  defp parse_datetime(%DateTime{} = value), do: value

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp normalize_baseline(nil), do: nil

  defp normalize_baseline(value) when is_map(value) do
    case parse_datetime(coverage_until(value)) do
      %DateTime{} = datetime -> Map.put(value, :coverage_until, datetime)
      nil -> value
    end
  end

  defp coverage_until(nil), do: nil

  defp coverage_until(value) when is_map(value),
    do: Map.get(value, :coverage_until, Map.get(value, "coverage_until"))

  defp coverage_until(_value), do: nil

  defp relative_opts(%__MODULE__{} = request) do
    [relative_to: request.relative_to, baseline: request.baseline]
  end

  defp present?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp to_keyword(map, keys) do
    Enum.reduce(keys, [], fn key, acc ->
      case Map.fetch(map, key) do
        {:ok, value} -> Keyword.put(acc, key, value)
        :error -> put_string_key(map, key, acc)
      end
    end)
  end

  defp put_string_key(map, key, acc) do
    case Map.fetch(map, Atom.to_string(key)) do
      {:ok, value} -> Keyword.put(acc, key, value)
      :error -> acc
    end
  end
end
