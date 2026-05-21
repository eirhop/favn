defmodule Favn.Contracts.RunnerError do
  @moduledoc """
  Redaction-safe runner error envelope.

  Runner implementations normalize exceptions, exits, throws, preflight
  diagnostics, and boundary errors into this contract before returning them to
  the orchestrator. `retryable?` is explicit so orchestrator retry policy does
  not inspect arbitrary error terms.
  """

  @type kind :: :error | :exit | :throw | :cancelled | :preflight | :boundary

  @type t :: %__MODULE__{
          kind: kind(),
          type: atom() | String.t(),
          message: String.t(),
          reason: String.t() | nil,
          details: map(),
          retryable?: boolean(),
          redacted?: boolean()
        }

  defstruct kind: :error,
            type: :runner_error,
            message: "Runner error",
            reason: nil,
            details: %{},
            retryable?: true,
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
      message: message(Map.get(fields, :message), reason),
      reason: safe_reason(reason),
      details: details(Map.get(fields, :details, %{})),
      retryable?: Map.get(fields, :retryable?, true),
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
      message: Keyword.get(opts, :message) || error_message(error) || "Runner error",
      reason: error,
      details: Keyword.get(opts, :details, %{}),
      retryable?: Keyword.get(opts, :retryable?, retryable_from_error(error))
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
      retryable?: false
    )
  end

  defp message(nil, reason), do: error_message(reason) || "Runner error"
  defp message(value, _reason), do: string_value(value)

  defp details(details) when is_map(details), do: redact(details)
  defp details(_details), do: %{}

  defp retryable_from_error(%{details: details}) when is_map(details) do
    Map.get(details, :asset_retryable?, Map.get(details, "asset_retryable?", true)) != false
  end

  defp retryable_from_error(%{"details" => details}) when is_map(details) do
    Map.get(details, "asset_retryable?", true) != false
  end

  defp retryable_from_error(_error), do: true

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

  defp safe_reason(nil), do: nil
  defp safe_reason(reason), do: inspect(reason, limit: 20, printable_limit: 4_096)

  defp string_value(value) when is_binary(value), do: value
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(value), do: inspect(value, limit: 20, printable_limit: 4_096)

  defp redact(value) when is_map(value) do
    Map.new(value, fn {key, map_value} -> {key, redact(key, map_value)} end)
  end

  defp redact(value) when is_list(value), do: Enum.map(value, &redact/1)

  defp redact(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&redact/1) |> List.to_tuple()

  defp redact(value) when is_binary(value), do: value
  defp redact(value) when is_atom(value), do: value
  defp redact(value) when is_number(value), do: value
  defp redact(value) when is_boolean(value), do: value
  defp redact(nil), do: nil
  defp redact(value), do: inspect(value, limit: 20, printable_limit: 4_096)

  defp redact(key, _value) when key in [:token, :password, :secret, :credential, :database],
    do: "[REDACTED]"

  defp redact(key, value) when is_atom(key),
    do: if(sensitive_key?(Atom.to_string(key)), do: "[REDACTED]", else: redact(value))

  defp redact(key, value) when is_binary(key),
    do: if(sensitive_key?(key), do: "[REDACTED]", else: redact(value))

  defp redact(_key, value), do: redact(value)

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)

    String.contains?(key, "token") or String.contains?(key, "password") or
      String.contains?(key, "secret") or String.contains?(key, "credential") or
      String.contains?(key, "database")
  end

  defp term_type(term) when is_map(term), do: :map
  defp term_type(term) when is_tuple(term), do: :tuple
  defp term_type(term) when is_list(term), do: :list
  defp term_type(term) when is_binary(term), do: :string
  defp term_type(term) when is_number(term), do: :number
  defp term_type(term) when is_boolean(term), do: :boolean
  defp term_type(_term), do: :term
end
