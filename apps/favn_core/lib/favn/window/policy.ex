defmodule Favn.Window.Policy do
  @moduledoc """
  Pipeline-level policy for turning run/schedule input into anchor windows.

  A policy is compiled from pipeline DSL clauses such as `window :monthly`.
  It is the default operational contract for a pipeline, not an asset-level
  ingestion mode.

  Supported policy kinds are `:hour`, `:day`, `:month`, and `:year`, with DSL
  aliases `:hourly`, `:daily`, `:monthly`, and `:yearly`.

  Scheduled policies support two explicit anchors:

  - `:previous_complete_period` selects the period immediately before the
    schedule occurrence and remains the default.
  - `:current_period` selects the period containing the schedule occurrence,
    even when that period is incomplete.

  Both anchors resolve relative to the schedule occurrence and effective
  timezone. Manual runs instead resolve explicit `%Favn.Window.Request{}` input
  such as `month:2026-03`.

  Windowed pipelines do not allow full-load submissions by default. Set
  `allow_full_load: true` only when a windowed pipeline should explicitly accept
  a no-window run.
  """

  alias Favn.TimePeriod
  alias Favn.Window.{Anchor, Request, Validate}

  @type kind :: Validate.kind()
  @type anchor_policy :: :previous_complete_period | :current_period

  @type t :: %__MODULE__{
          kind: kind(),
          anchor: anchor_policy(),
          timezone: String.t() | nil,
          timezone_source: Favn.Window.Spec.timezone_source(),
          lookback: non_neg_integer(),
          allow_full_load: boolean()
        }

  defstruct [
    :kind,
    :timezone,
    :timezone_source,
    anchor: :previous_complete_period,
    lookback: 0,
    allow_full_load: false
  ]

  @spec new(atom(), keyword()) :: {:ok, t()} | {:error, term()}
  @doc """
  Builds a validated pipeline window policy.

  ## Examples

      iex> Favn.Window.Policy.new(:monthly)
      {:ok, %Favn.Window.Policy{kind: :month, anchor: :previous_complete_period, timezone: nil, timezone_source: nil, lookback: 0, allow_full_load: false}}

      iex> Favn.Window.Policy.new(:daily, timezone: "Europe/Oslo")
      {:ok, %Favn.Window.Policy{kind: :day, anchor: :previous_complete_period, timezone: "Europe/Oslo", timezone_source: :local, lookback: 0, allow_full_load: false}}
  """
  def new(kind, opts \\ []) when is_list(opts) do
    with :ok <-
           Validate.strict_keyword_opts(opts, [:anchor, :timezone, :lookback, :allow_full_load]),
         {:ok, normalized_kind} <- normalize_kind(kind),
         anchor <- Keyword.get(opts, :anchor, :previous_complete_period),
         :ok <- validate_anchor(anchor),
         timezone <- Keyword.get(opts, :timezone),
         :ok <- validate_optional_timezone(timezone),
         lookback <- Keyword.get(opts, :lookback, 0),
         :ok <- validate_lookback(lookback),
         allow_full_load <- Keyword.get(opts, :allow_full_load, false),
         :ok <- validate_boolean(:allow_full_load, allow_full_load) do
      {:ok,
       %__MODULE__{
         kind: normalized_kind,
         anchor: anchor,
         timezone: timezone,
         timezone_source: if(is_binary(timezone), do: :local),
         lookback: lookback,
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
      |> maybe_put(:lookback, field_value(value, :lookback))
      |> maybe_put(:allow_full_load, field_value(value, :allow_full_load))

    with {:ok, decoded_kind} <- decode_kind(kind),
         {:ok, decoded_opts} <- decode_opts(opts),
         {:ok, policy} <- new(decoded_kind, decoded_opts),
         {:ok, source} <- decode_timezone_source(field_value(value, :timezone_source)) do
      {:ok, %{policy | timezone_source: source || policy.timezone_source}}
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
         :ok <- validate_timezone_source(policy.timezone, policy.timezone_source),
         :ok <- validate_lookback(policy.lookback),
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
  Resolves a scheduled occurrence using the policy's explicit anchor.

  The policy must already carry its independently resolved effective timezone.
  The schedule timezone argument is retained for caller compatibility and is
  intentionally ignored.
  """
  def resolve_scheduled(
        %__MODULE__{anchor: anchor} = policy,
        %DateTime{} = due_at,
        _schedule_timezone
      )
      when anchor in [:previous_complete_period, :current_period] do
    with timezone when is_binary(timezone) <- policy.timezone,
         :ok <- Validate.timezone(timezone),
         {:ok, period} <- scheduled_period(anchor, policy.kind, due_at, timezone) do
      Anchor.new(period.kind, period.start_at, period.end_at, timezone: period.timezone)
    else
      nil -> {:error, :unresolved_pipeline_timezone}
      {:error, _reason} = error -> error
    end
  end

  @spec normalize_kind(term()) :: {:ok, kind()} | {:error, term()}
  def normalize_kind(kind) do
    case TimePeriod.normalize_kind(kind) do
      {:ok, normalized_kind} -> {:ok, normalized_kind}
      {:error, {:invalid_period_kind, other}} -> {:error, {:invalid_window_policy_kind, other}}
    end
  end

  defp ensure_matching_kind(%__MODULE__{kind: kind}, %Request{kind: kind}), do: :ok

  defp ensure_matching_kind(%__MODULE__{kind: expected}, %Request{kind: actual}),
    do: {:error, {:window_kind_mismatch, expected, actual}}

  defp default_timezone(%__MODULE__{timezone: timezone}) when is_binary(timezone), do: timezone
  defp default_timezone(%__MODULE__{}), do: "Etc/UTC"

  defp validate_anchor(:previous_complete_period), do: :ok
  defp validate_anchor(:current_period), do: :ok
  defp validate_anchor(anchor), do: {:error, {:invalid_anchor_policy, anchor}}

  defp scheduled_period(:previous_complete_period, kind, due_at, timezone),
    do: TimePeriod.previous_complete(kind, due_at, timezone)

  defp scheduled_period(:current_period, kind, due_at, timezone),
    do: TimePeriod.current(kind, due_at, timezone)

  defp validate_optional_timezone(nil), do: :ok
  defp validate_optional_timezone(timezone), do: Validate.timezone(timezone)

  defp validate_lookback(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_lookback(value), do: {:error, {:invalid_lookback, value}}

  defp validate_timezone_source(nil, nil), do: :ok

  defp validate_timezone_source(timezone, source)
       when is_binary(timezone) and source in [:local, :application_default, :utc_fallback],
       do: :ok

  defp validate_timezone_source(_timezone, source),
    do: {:error, {:invalid_pipeline_window_timezone_source, source}}

  @doc false
  @spec resolve_timezone(t(), String.t(), :application_default | :utc_fallback) ::
          {:ok, t()} | {:error, term()}
  def resolve_timezone(%__MODULE__{timezone: timezone} = policy, default, default_source) do
    effective = timezone || default
    source = policy.timezone_source || default_source

    with :ok <- Validate.timezone(effective),
         :ok <- validate_timezone_source(effective, source) do
      {:ok, %{policy | timezone: effective, timezone_source: source}}
    end
  end

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

      {:anchor, "current_period"}, {:ok, acc} ->
        {:cont, {:ok, Keyword.put(acc, :anchor, :current_period)}}

      {key, value}, {:ok, acc} ->
        {:cont, {:ok, Keyword.put(acc, key, value)}}
    end)
  rescue
    ArgumentError -> {:error, {:invalid_window_policy_opts, opts}}
  end

  defp decode_timezone_source(nil), do: {:ok, nil}

  defp decode_timezone_source(source)
       when source in [:local, :application_default, :utc_fallback], do: {:ok, source}

  defp decode_timezone_source("local"), do: {:ok, :local}
  defp decode_timezone_source("application_default"), do: {:ok, :application_default}
  defp decode_timezone_source("utc_fallback"), do: {:ok, :utc_fallback}

  defp decode_timezone_source(source),
    do: {:error, {:invalid_pipeline_window_timezone_source, source}}

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp field_value(value, field) when is_map(value) do
    Map.get(value, field, Map.get(value, Atom.to_string(field)))
  end
end
