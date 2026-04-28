defmodule Favn.Window.Policy do
  @moduledoc """
  Pipeline-level policy for turning run/schedule input into anchor windows.

  A policy is compiled from pipeline DSL clauses such as `window :monthly`.
  It is the default operational contract for a pipeline, not an asset-level
  ingestion mode.

  Supported policy kinds are `:hour`, `:day`, `:month`, and `:year`, with DSL
  aliases `:hourly`, `:daily`, `:monthly`, and `:yearly`.

  The V1 policy anchor is `:previous_complete_period`. Scheduled runs resolve
  that policy relative to the schedule occurrence and schedule timezone. Manual
  runs resolve explicit `%Favn.Window.Request{}` input such as `month:2026-03`.

  Windowed pipelines do not allow full-load submissions by default. Set
  `allow_full_load: true` only when a windowed pipeline should explicitly accept
  a no-window run.
  """

  alias Favn.Window.{Anchor, Request, Validate}

  @type kind :: Validate.kind()
  @type anchor_policy :: :previous_complete_period

  @type t :: %__MODULE__{
          kind: kind(),
          anchor: anchor_policy(),
          timezone: String.t() | nil,
          allow_full_load: boolean()
        }

  defstruct [:kind, anchor: :previous_complete_period, timezone: nil, allow_full_load: false]

  @spec new(atom(), keyword()) :: {:ok, t()} | {:error, term()}
  @doc """
  Builds a validated pipeline window policy.

  ## Examples

      iex> Favn.Window.Policy.new(:monthly)
      {:ok, %Favn.Window.Policy{kind: :month, anchor: :previous_complete_period, timezone: nil, allow_full_load: false}}

      iex> Favn.Window.Policy.new(:daily, timezone: "Europe/Oslo")
      {:ok, %Favn.Window.Policy{kind: :day, anchor: :previous_complete_period, timezone: "Europe/Oslo", allow_full_load: false}}
  """
  def new(kind, opts \\ []) when is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:anchor, :timezone, :allow_full_load]),
         {:ok, normalized_kind} <- normalize_kind(kind),
         anchor <- Keyword.get(opts, :anchor, :previous_complete_period),
         :ok <- validate_anchor(anchor),
         timezone <- Keyword.get(opts, :timezone),
         :ok <- validate_optional_timezone(timezone),
         allow_full_load <- Keyword.get(opts, :allow_full_load, false),
         :ok <- validate_boolean(:allow_full_load, allow_full_load) do
      {:ok,
       %__MODULE__{
         kind: normalized_kind,
         anchor: anchor,
         timezone: timezone,
         allow_full_load: allow_full_load
       }}
    end
  end

  @spec new!(atom(), keyword()) :: t()
  def new!(kind, opts \\ []) do
    case new(kind, opts) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid window policy: #{inspect(reason)}"
    end
  end

  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = policy), do: validate(policy)
  def from_value(kind) when is_atom(kind), do: new(kind)

  def from_value(kind) when is_binary(kind) do
    with {:ok, decoded_kind} <- decode_kind(kind) do
      new(decoded_kind)
    end
  end

  def from_value(value) when is_map(value) do
    kind = field_value(value, :kind)

    opts =
      []
      |> maybe_put(:anchor, field_value(value, :anchor))
      |> maybe_put(:timezone, field_value(value, :timezone))
      |> maybe_put(:allow_full_load, field_value(value, :allow_full_load))

    with {:ok, decoded_kind} <- decode_kind(kind),
         {:ok, decoded_opts} <- decode_opts(opts) do
      new(decoded_kind, decoded_opts)
    end
  end

  def from_value(other), do: {:error, {:invalid_window_policy, other}}

  @spec from_value!(term()) :: t() | nil
  def from_value!(value) do
    case from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid window policy: #{inspect(reason)}"
    end
  end

  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = policy) do
    with :ok <- Validate.kind(policy.kind),
         :ok <- validate_anchor(policy.anchor),
         :ok <- validate_optional_timezone(policy.timezone),
         :ok <- validate_boolean(:allow_full_load, policy.allow_full_load) do
      {:ok, policy}
    end
  end

  @spec resolve_manual(t() | nil, Request.t() | map() | nil) ::
          {:ok, Anchor.t() | nil} | {:error, term()}
  @doc """
  Resolves manual run input against a pipeline policy.

  A nil policy with nil request is a full-load style run and resolves to nil.
  A windowed policy without a request returns an error unless the policy allows
  full-load submissions.
  """
  def resolve_manual(nil, nil), do: {:ok, nil}

  def resolve_manual(nil, %Request{mode: :full_load}), do: {:ok, nil}

  def resolve_manual(nil, %Request{} = request),
    do: {:error, {:window_request_without_policy, request.kind}}

  def resolve_manual(%__MODULE__{allow_full_load: true}, nil), do: {:ok, nil}

  def resolve_manual(%__MODULE__{} = policy, nil),
    do: {:error, {:missing_window_request, policy.kind}}

  def resolve_manual(%__MODULE__{} = policy, %Request{mode: :full_load}) do
    if policy.allow_full_load,
      do: {:ok, nil},
      else: {:error, {:full_load_not_allowed, policy.kind}}
  end

  def resolve_manual(%__MODULE__{} = policy, %Request{mode: :single} = request) do
    case ensure_matching_kind(policy, request) do
      :ok -> Request.to_anchor(request, default_timezone(policy))
      {:error, _reason} = error -> error
    end
  end

  def resolve_manual(policy, request) when is_map(request) do
    with {:ok, request} <- Request.from_value(request) do
      resolve_manual(policy, request)
    end
  end

  @spec resolve_scheduled(t(), DateTime.t(), String.t() | nil) ::
          {:ok, Anchor.t()} | {:error, term()}
  @doc """
  Resolves a scheduled occurrence into the previous complete anchor period.

  The policy timezone wins when present; otherwise the schedule timezone is used,
  falling back to `"Etc/UTC"`.
  """
  def resolve_scheduled(
        %__MODULE__{anchor: :previous_complete_period} = policy,
        %DateTime{} = due_at,
        schedule_timezone
      ) do
    timezone = policy.timezone || schedule_timezone || "Etc/UTC"

    with :ok <- Validate.timezone(timezone) do
      local_due = DateTime.shift_zone!(due_at, timezone, Favn.Timezone.database!())
      end_at = floor_to_kind(local_due, policy.kind)
      start_at = shift_kind(end_at, policy.kind, -1)

      Anchor.new(policy.kind, start_at, end_at, timezone: timezone)
    end
  end

  @spec normalize_kind(term()) :: {:ok, kind()} | {:error, term()}
  def normalize_kind(kind) when kind in [:hour, :hourly], do: {:ok, :hour}
  def normalize_kind(kind) when kind in [:day, :daily], do: {:ok, :day}
  def normalize_kind(kind) when kind in [:month, :monthly], do: {:ok, :month}
  def normalize_kind(kind) when kind in [:year, :yearly], do: {:ok, :year}
  def normalize_kind(other), do: {:error, {:invalid_window_policy_kind, other}}

  defp ensure_matching_kind(%__MODULE__{kind: kind}, %Request{kind: kind}), do: :ok

  defp ensure_matching_kind(%__MODULE__{kind: expected}, %Request{kind: actual}),
    do: {:error, {:window_kind_mismatch, expected, actual}}

  defp default_timezone(%__MODULE__{timezone: timezone}) when is_binary(timezone), do: timezone
  defp default_timezone(%__MODULE__{}), do: "Etc/UTC"

  defp validate_anchor(:previous_complete_period), do: :ok
  defp validate_anchor(anchor), do: {:error, {:invalid_anchor_policy, anchor}}

  defp validate_optional_timezone(nil), do: :ok
  defp validate_optional_timezone(timezone), do: Validate.timezone(timezone)

  defp validate_boolean(_field, value) when is_boolean(value), do: :ok
  defp validate_boolean(field, value), do: {:error, {:invalid_boolean, field, value}}

  defp decode_kind(value) when is_atom(value), do: {:ok, value}
  defp decode_kind("hour"), do: {:ok, :hour}
  defp decode_kind("hourly"), do: {:ok, :hourly}
  defp decode_kind("day"), do: {:ok, :day}
  defp decode_kind("daily"), do: {:ok, :daily}
  defp decode_kind("month"), do: {:ok, :month}
  defp decode_kind("monthly"), do: {:ok, :monthly}
  defp decode_kind("year"), do: {:ok, :year}
  defp decode_kind("yearly"), do: {:ok, :yearly}
  defp decode_kind(value), do: {:error, {:invalid_window_policy_kind, value}}

  defp decode_opts(opts) do
    Enum.reduce_while(opts, {:ok, []}, fn
      {:anchor, "previous_complete_period"}, {:ok, acc} ->
        {:cont, {:ok, Keyword.put(acc, :anchor, :previous_complete_period)}}

      {key, value}, {:ok, acc} ->
        {:cont, {:ok, Keyword.put(acc, key, value)}}
    end)
  rescue
    ArgumentError -> {:error, {:invalid_window_policy_opts, opts}}
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp field_value(value, field) when is_map(value) do
    Map.get(value, field, Map.get(value, Atom.to_string(field)))
  end

  defp floor_to_kind(datetime, :hour), do: %{datetime | minute: 0, second: 0, microsecond: {0, 0}}

  defp floor_to_kind(datetime, :day),
    do: %{datetime | hour: 0, minute: 0, second: 0, microsecond: {0, 0}}

  defp floor_to_kind(datetime, :month),
    do: %{datetime | day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 0}}

  defp floor_to_kind(datetime, :year),
    do: %{datetime | month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 0}}

  defp shift_kind(datetime, :hour, count), do: DateTime.add(datetime, count * 3600, :second)
  defp shift_kind(datetime, :day, count), do: shift_day(datetime, count)
  defp shift_kind(datetime, :month, count), do: shift_month(datetime, count)
  defp shift_kind(datetime, :year, count), do: shift_year(datetime, count)

  defp shift_day(%DateTime{} = datetime, count) do
    date = datetime |> DateTime.to_date() |> Date.add(count)
    datetime_from_date!(date.year, date.month, date.day, datetime.time_zone)
  end

  defp shift_month(%DateTime{} = datetime, count) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    year = div(total, 12)
    month = rem(total, 12) + 1
    datetime_from_date!(year, month, 1, datetime.time_zone)
  end

  defp shift_year(%DateTime{} = datetime, count) do
    date = DateTime.to_date(datetime)
    datetime_from_date!(date.year + count, 1, 1, datetime.time_zone)
  end

  defp datetime_from_date!(year, month, day, timezone) do
    {:ok, date} = Date.new(year, month, day)
    {:ok, naive} = NaiveDateTime.new(date, ~T[00:00:00])
    DateTime.from_naive!(naive, timezone, Favn.Timezone.database!())
  end
end
