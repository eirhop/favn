defmodule Favn.SQL.Retry.Classification do
  @moduledoc """
  Classifies normalized SQL runtime errors for safe retry decisions.

  Classification is deliberately separate from adapter error mapping. Adapter
  errors describe what failed; retry classification decides whether Favn may try
  the operation again for the caller supplied phase.
  """

  alias Favn.SQL.Error

  @enforce_keys [:class, :reason, :retryable?, :safe_phase?, :phase]
  defstruct [
    :class,
    :reason,
    :retryable?,
    :safe_phase?,
    :phase,
    :sqlstate,
    :suggestion
  ]

  @type class ::
          :transient
          | :capacity
          | :permanent_config
          | :unsafe_unknown_commit_state
          | :user_sql_error

  @type phase :: atom() | nil

  @type t :: %__MODULE__{
          class: class(),
          reason: atom(),
          retryable?: boolean(),
          safe_phase?: boolean(),
          phase: phase(),
          sqlstate: String.t() | nil,
          suggestion: String.t() | nil
        }

  @safe_phases MapSet.new([
                 :session_creation,
                 :session_bootstrap,
                 :bootstrap,
                 :connect,
                 :read_only,
                 :metadata_read,
                 :introspection
               ])

  @doc """
  Classifies a SQL error for retry.

  Pass `:phase` to state where the operation failed. Automatic retry is only
  safe for known pre-SQL/bootstrap phases or explicit read-only phases. Passing
  `safe_phase?: true` may be used by callers that have already proven idempotent
  or read-only semantics for a custom phase.
  """
  @spec classify(Error.t(), keyword()) :: t()
  def classify(%Error{} = error, opts \\ []) when is_list(opts) do
    phase = Keyword.get(opts, :phase)
    safe_phase? = safe_phase?(phase, opts)
    sqlstate = normalize_sqlstate(error.sqlstate)
    message = String.downcase(error.message || "")

    {class, reason, retryable?, suggestion} = classify_error(error, sqlstate, message)

    %__MODULE__{
      class: class,
      reason: reason,
      retryable?: retryable?,
      safe_phase?: safe_phase?,
      phase: phase,
      sqlstate: sqlstate,
      suggestion: suggestion
    }
  end

  @doc """
  Returns true when the classification permits retry for its current phase.
  """
  @spec retryable_in_phase?(t()) :: boolean()
  def retryable_in_phase?(%__MODULE__{} = classification),
    do: classification.retryable? and classification.safe_phase?

  defp classify_error(%Error{} = error, sqlstate, message) do
    cond do
      unknown_commit_state?(error, message) ->
        {:unsafe_unknown_commit_state, :unknown_commit_state, false,
         "Do not retry automatically; inspect transaction outcome before continuing."}

      adapter_capacity?(error) ->
        {:capacity, :adapter_capacity, true,
         "Retry with capacity backoff or reduce concurrent SQL metadata connections."}

      capacity_sqlstate?(sqlstate) ->
        {:capacity, :metadata_store_connection_exhausted, true,
         "Retry with capacity backoff or reduce concurrent SQL metadata connections."}

      capacity_message?(message) ->
        {:capacity, capacity_reason(message), true,
         "Retry with capacity backoff or reduce concurrent SQL connections."}

      permanent_config?(error) ->
        {:permanent_config, error.type, false,
         "Fix SQL connection configuration before retrying."}

      transient?(error, message) ->
        {:transient, transient_reason(error, message), true,
         "Retry from a safe phase with bounded exponential backoff."}

      true ->
        {:user_sql_error, user_sql_reason(error), false,
         "Fix the SQL statement or referenced relation before retrying."}
    end
  end

  defp safe_phase?(phase, opts) do
    Keyword.get_lazy(opts, :safe_phase?, fn -> MapSet.member?(@safe_phases, phase) end)
  end

  defp normalize_sqlstate(nil), do: nil
  defp normalize_sqlstate(sqlstate) when is_binary(sqlstate), do: String.upcase(sqlstate)
  defp normalize_sqlstate(sqlstate), do: sqlstate |> to_string() |> String.upcase()

  defp unknown_commit_state?(%Error{} = error, message) do
    classification = Map.get(error.details || %{}, :classification)
    transaction_stage = Map.get(error.details || %{}, :transaction_stage)

    classification in [:unknown_commit_state, :unknown_outcome_timeout] or
      transaction_stage == :commit or
      String.contains?(message, "unknown commit") or
      String.contains?(message, "outcome is unknown")
  end

  defp adapter_capacity?(%Error{} = error) do
    Map.get(error.details || %{}, :classification) == :capacity
  end

  defp capacity_sqlstate?("53300"), do: true
  defp capacity_sqlstate?(_sqlstate), do: false

  defp capacity_message?(message) do
    String.contains?(message, "too many clients") or
      String.contains?(message, "remaining connection slots are reserved") or
      String.contains?(message, "reserved connection") or
      (String.contains?(message, "pgbouncer") and
         (String.contains?(message, "pool") or
            String.contains?(message, "timeout") or
            String.contains?(message, "no more connections")))
  end

  defp capacity_reason(message) do
    if String.contains?(message, "pgbouncer") do
      :pgbouncer_pool_exhausted
    else
      :metadata_store_connection_exhausted
    end
  end

  defp permanent_config?(%Error{type: type}) do
    type in [
      :invalid_config,
      :authentication_error,
      :unsupported_capability,
      :introspection_mismatch
    ]
  end

  defp transient?(%Error{retryable?: true}, _message), do: true
  defp transient?(%Error{type: :connection_error}, _message), do: true
  defp transient?(%Error{type: :admission_timeout}, _message), do: true

  defp transient?(_error, message) do
    String.contains?(message, "connection refused") or
      String.contains?(message, "connection timeout") or
      String.contains?(message, "timed out") or
      String.contains?(message, "timeout")
  end

  defp transient_reason(%Error{type: :admission_timeout}, _message), do: :admission_timeout
  defp transient_reason(%Error{retryable?: true}, _message), do: :adapter_retryable
  defp transient_reason(%Error{type: :connection_error}, _message), do: :connection_error
  defp transient_reason(_error, _message), do: :connection_timeout

  defp user_sql_reason(%Error{type: :missing_relation}), do: :missing_relation
  defp user_sql_reason(%Error{type: :execution_error}), do: :execution_error
  defp user_sql_reason(%Error{type: type}), do: type
end
