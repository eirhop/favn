defmodule Favn.Contracts.RunnerError do
  @moduledoc """
  Redaction-safe runner error envelope.

  Runner implementations normalize exceptions, exits, throws, preflight
  diagnostics, and boundary errors into this contract before returning them to
  the orchestrator. `retryable?` is explicit so orchestrator retry policy does
  not inspect arbitrary error terms.
  """

  @type kind :: :error | :exit | :throw | :cancelled | :preflight | :boundary
  @type outcome :: :safe_failure | :unknown | :cancelled

  @operational_untrusted_keys [:reason, :message, :detail, :details, :error, :exception]
  @sensitive_fragments [
    "token",
    "password",
    "secret",
    "authorization",
    "cookie",
    "credential",
    "database",
    "dsn",
    "url",
    "uri",
    "api_key",
    "apikey",
    "access_key",
    "accesskey",
    "private_key",
    "privatekey"
  ]
  @sensitive_assignment ~r/(token|password|secret|authorization|cookie|credential|database|dsn|url|uri|api_key|apikey|access_key|accesskey|private_key|privatekey)\s*[:=]\s*((?:Bearer\s+)?[^\s,;]+)/i
  @bearer_token ~r/(bearer)\s+([^\s,;]+)/i
  @url_userinfo ~r/([a-z][a-z0-9+.-]*:\/\/)([^\s\/@:]+):([^\s\/@]+)@([^\s,;]+)/i

  @type t :: %__MODULE__{
          kind: kind(),
          type: atom() | String.t(),
          phase: atom() | String.t() | nil,
          message: String.t(),
          reason: String.t() | nil,
          details: map(),
          retryable?: boolean(),
          retry_after_ms: non_neg_integer() | nil,
          outcome: outcome(),
          redacted?: boolean()
        }

  defstruct kind: :error,
            type: :runner_error,
            phase: nil,
            message: "Runner error",
            reason: nil,
            details: %{},
            retryable?: false,
            retry_after_ms: nil,
            outcome: :unknown,
            redacted?: true

  @doc """
  Builds a runner error envelope from explicit fields.
  """
  @spec new(keyword() | map()) :: t()
  def new(fields \\ []) when is_map(fields) or is_list(fields) do
    fields = Map.new(fields)
    reason = Map.get(fields, :reason)

    struct!(__MODULE__, %{
      kind: Map.get(fields, :kind, :error),
      type: Map.get(fields, :type, type_from_reason(reason)),
      phase: Map.get(fields, :phase, phase_from_reason(reason)),
      message: message(Map.get(fields, :message), reason),
      reason: safe_reason(reason),
      details: details(Map.get(fields, :details, %{})),
      retryable?: Map.get(fields, :retryable?, false),
      retry_after_ms: normalize_retry_after(Map.get(fields, :retry_after_ms)),
      outcome: normalize_outcome(Map.get(fields, :outcome), Map.get(fields, :retryable?, false)),
      redacted?: Map.get(fields, :redacted?, true)
    })
  end

  @doc """
  Normalizes an arbitrary error term into a redaction-safe envelope.
  """
  @spec normalize(term(), keyword()) :: t()
  def normalize(error, opts \\ [])
  def normalize(%__MODULE__{} = error, _opts), do: error

  def normalize(error, opts) when is_list(opts) do
    new(
      kind: Keyword.get(opts, :kind, :error),
      type: Keyword.get(opts, :type, type_from_reason(error)),
      phase: Keyword.get(opts, :phase, phase_from_reason(error)),
      message: Keyword.get(opts, :message) || error_message(error) || "Runner error",
      reason: error,
      details: Keyword.get(opts, :details, details_from_error(error)),
      retryable?: Keyword.get(opts, :retryable?, retryable_from_error(error)),
      retry_after_ms: Keyword.get(opts, :retry_after_ms, retry_after_from_error(error)),
      outcome: Keyword.get(opts, :outcome, outcome_from_error(error))
    )
  end

  @doc """
  Normalizes a rescue/catch payload.
  """
  @spec exception(kind(), term(), [term()]) :: t()
  def exception(kind, reason, stacktrace \\ []) when kind in [:error, :exit, :throw] do
    normalize(reason,
      kind: kind,
      type: type_from_reason(reason),
      details: %{stacktrace_depth: length(stacktrace)}
    )
  end

  @doc """
  Builds a cancellation error envelope.
  """
  @spec cancelled(term()) :: t()
  def cancelled(reason) do
    normalize(reason,
      kind: :cancelled,
      type: :cancelled,
      message: "Runner execution cancelled",
      retryable?: false,
      outcome: :cancelled
    )
  end

  defp message(nil, reason), do: sanitize_text(error_message(reason) || "Runner error")
  defp message(value, _reason), do: value |> string_value() |> sanitize_text()

  defp details(details) when is_map(details), do: sanitize_value(details)
  defp details(_details), do: %{}

  defp details_from_error(%_{} = error) do
    error
    |> Map.from_struct()
    |> Map.drop([:message, :reason, :stack, :stacktrace])
  end

  defp details_from_error(%{details: details}) when is_map(details), do: details
  defp details_from_error(%{"details" => details}) when is_map(details), do: details

  defp details_from_error(%{} = error),
    do:
      Map.drop(error, [
        :kind,
        "kind",
        :type,
        "type",
        :phase,
        "phase",
        :message,
        "message",
        :reason,
        "reason",
        :stack,
        "stack",
        :stacktrace,
        "stacktrace"
      ])

  defp details_from_error(_error), do: %{}

  defp retryable_from_error(%{details: details}) when is_map(details) do
    Map.get(details, :asset_retryable?, Map.get(details, "asset_retryable?", false)) == true
  end

  defp retryable_from_error(%{"details" => details}) when is_map(details) do
    Map.get(details, "asset_retryable?", false) == true
  end

  defp retryable_from_error(%{retryable?: value}) when is_boolean(value), do: value
  defp retryable_from_error(%{"retryable?" => value}) when is_boolean(value), do: value
  defp retryable_from_error(_error), do: false

  defp retry_after_from_error(%{retry_after_ms: value}), do: value
  defp retry_after_from_error(%{"retry_after_ms" => value}), do: value

  defp retry_after_from_error(%{details: details}) when is_map(details),
    do: Map.get(details, :retry_after_ms, Map.get(details, "retry_after_ms"))

  defp retry_after_from_error(%{"details" => details}) when is_map(details),
    do: Map.get(details, "retry_after_ms")

  defp retry_after_from_error(_error), do: nil

  defp outcome_from_error(%{outcome: outcome}), do: outcome
  defp outcome_from_error(%{"outcome" => outcome}), do: outcome

  defp outcome_from_error(_error), do: :unknown

  defp normalize_retry_after(value)
       when is_integer(value) and value >= 0 and value <= 86_400_000,
       do: value

  defp normalize_retry_after(_value), do: nil

  defp normalize_outcome(value, _retryable?) when value in [:safe_failure, :unknown, :cancelled],
    do: value

  defp normalize_outcome("safe_failure", _retryable?), do: :safe_failure
  defp normalize_outcome("unknown", _retryable?), do: :unknown
  defp normalize_outcome("cancelled", _retryable?), do: :cancelled
  defp normalize_outcome(_value, _retryable?), do: :unknown

  defp error_message(%{__exception__: true} = exception) do
    Exception.message(exception)
  rescue
    _error -> nil
  end

  defp error_message(%{message: message}) when is_binary(message), do: message
  defp error_message(%{"message" => message}) when is_binary(message), do: message
  defp error_message(_error), do: nil

  defp type_from_reason(%{__exception__: true, __struct__: module}), do: module
  defp type_from_reason(%{type: type}), do: type
  defp type_from_reason(%{"type" => type}), do: type
  defp type_from_reason(reason) when is_atom(reason), do: reason
  defp type_from_reason(reason), do: term_type(reason)

  defp phase_from_reason(%{phase: phase}), do: phase
  defp phase_from_reason(%{"phase" => phase}), do: phase
  defp phase_from_reason(_reason), do: nil

  defp safe_reason(nil), do: nil

  defp safe_reason(%{__exception__: true} = exception),
    do: exception |> error_message() |> sanitize_text()

  defp safe_reason(%_{} = reason) do
    (error_message(reason) || inspect_value(type_from_reason(reason)))
    |> sanitize_text()
  end

  defp safe_reason(%{} = reason),
    do: reason |> reason_only() |> sanitize_value() |> inspect_value()

  defp safe_reason(reason), do: reason |> sanitize_value() |> inspect_value()

  defp reason_only(%{reason: reason}), do: reason
  defp reason_only(%{"reason" => reason}), do: reason
  defp reason_only(reason), do: reason

  defp string_value(value) when is_binary(value), do: value
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value), do: inspect_value(value)

  defp inspect_value(value), do: inspect(value, limit: 20, printable_limit: 4_096)

  defp sanitize_value(%DateTime{} = value), do: value

  defp sanitize_value(%_{} = value) do
    value
    |> Map.from_struct()
    |> sanitize_value()
  end

  defp sanitize_value(value) when is_map(value) do
    Map.new(value, fn {key, map_value} -> {key, sanitize_value(key, map_value)} end)
  end

  defp sanitize_value(value) when is_list(value), do: Enum.map(value, &sanitize_value/1)

  defp sanitize_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&sanitize_value/1) |> List.to_tuple()

  defp sanitize_value(value) when is_binary(value), do: value
  defp sanitize_value(value) when is_atom(value), do: value
  defp sanitize_value(value) when is_number(value), do: value
  defp sanitize_value(value), do: inspect_value(value)

  defp sanitize_value(key, value) when key in @operational_untrusted_keys,
    do: sanitize_untrusted_value(value)

  defp sanitize_value(key, _value)
       when key in [:token, :password, :secret, :credential, :database],
       do: :redacted

  defp sanitize_value(key, value) when key in [:secret?, "secret?"], do: sanitize_value(value)

  defp sanitize_value(key, value) when is_atom(key),
    do: key |> Atom.to_string() |> sanitize_value(value)

  defp sanitize_value(key, value) when is_binary(key),
    do: sanitize_keyed_value(key, value)

  defp sanitize_value(_key, value), do: sanitize_value(value)

  defp sanitize_keyed_value(key, value) do
    cond do
      operational_untrusted_key?(key) -> sanitize_untrusted_value(value)
      sensitive_key?(key) -> :redacted
      true -> sanitize_value(value)
    end
  end

  defp sanitize_untrusted_value(%DateTime{} = value), do: value

  defp sanitize_untrusted_value(%_{} = value),
    do: value |> Map.from_struct() |> sanitize_untrusted_value()

  defp sanitize_untrusted_value(value) when is_map(value) do
    Map.new(value, fn {key, map_value} -> {key, sanitize_value(key, map_value)} end)
  end

  defp sanitize_untrusted_value(value) when is_list(value),
    do: Enum.map(value, &sanitize_untrusted_value/1)

  defp sanitize_untrusted_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&sanitize_untrusted_value/1)
    |> List.to_tuple()
  end

  defp sanitize_untrusted_value(value) when is_binary(value), do: sanitize_text(value)
  defp sanitize_untrusted_value(value) when is_atom(value), do: value
  defp sanitize_untrusted_value(value) when is_number(value), do: value
  defp sanitize_untrusted_value(value), do: value |> inspect_value() |> sanitize_text()

  defp sanitize_text(nil), do: nil

  defp sanitize_text(value) when is_binary(value) do
    value
    |> String.replace(@url_userinfo, "[REDACTED_URL]")
    |> String.replace(@bearer_token, "\\1 [REDACTED]")
    |> String.replace(@sensitive_assignment, "\\1=[REDACTED]")
  end

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@sensitive_fragments, &String.contains?(key, &1))
  end

  defp operational_untrusted_key?(key) when is_binary(key) do
    key = String.downcase(key)
    Enum.any?(@operational_untrusted_keys, &(key == Atom.to_string(&1)))
  end

  defp term_type(term) when is_map(term), do: :map
  defp term_type(term) when is_tuple(term), do: :tuple
  defp term_type(term) when is_list(term), do: :list
  defp term_type(term) when is_binary(term), do: :string
  defp term_type(term) when is_number(term), do: :number
  defp term_type(_term), do: :term
end
