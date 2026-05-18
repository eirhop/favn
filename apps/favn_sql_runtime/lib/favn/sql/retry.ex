defmodule Favn.SQL.Retry do
  @moduledoc """
  Safe retry wrapper for SQL runtime operations.

  `run/2` never retries unless the error classification is retryable and the
  caller supplied phase is safe for automatic retry. This prevents accidental
  retries of SQL writes, materialization, or transaction work where the outcome
  may already have committed.
  """

  alias Favn.SQL.Error
  alias Favn.SQL.Observability
  alias Favn.SQL.Retry.Classification
  alias Favn.SQL.Retry.Policy

  @type result :: {:ok, term()} | {:error, term()}

  @doc """
  Runs a zero-arity function with bounded safe retry.

  Options:

    * `:phase` - caller supplied phase used by classification safety checks.
    * `:safe_phase?` - explicit safety override for custom read-only/idempotent phases.
    * `:policy` - `Favn.SQL.Retry.Policy` struct or keyword overrides.
    * `:sleep_fun` - function called with each delay in milliseconds.
    * `:random_fun` - zero-arity function returning a float between `0.0` and `1.0`.

  The wrapped function should return `{:ok, value}` or `{:error, reason}`. Raised
  exceptions and exits are normalized into `%Favn.SQL.Error{}` values.
  """
  @spec run((-> result()), keyword()) :: result()
  def run(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    policy = opts |> Keyword.get(:policy) |> Policy.new()
    sleep_fun = Keyword.get(opts, :sleep_fun, &Process.sleep/1)
    random_fun = Keyword.get(opts, :random_fun, &:rand.uniform/0)

    do_run(fun, opts, policy, sleep_fun, random_fun, 1, [])
  end

  @doc """
  Three-arity variant that accepts a policy separately from run options.
  """
  @spec run((-> result()), Policy.t() | keyword(), keyword()) :: result()
  def run(fun, policy_or_opts, opts)
      when is_function(fun, 0) and is_list(opts) do
    run(fun, Keyword.put(opts, :policy, policy_or_opts))
  end

  defp do_run(fun, opts, %Policy{} = policy, sleep_fun, random_fun, attempt, delays) do
    case call(fun) do
      {:ok, _value} = ok ->
        ok

      {:error, %Error{} = error} ->
        classification = Classification.classify(error, opts)

        if retry?(classification, attempt, policy) do
          delay = Policy.delay_ms(policy, classification, attempt, random_fun)
          emit_retry_attempt(classification, attempt, delay)
          sleep_fun.(delay)
          do_run(fun, opts, policy, sleep_fun, random_fun, attempt + 1, [delay | delays])
        else
          {:error,
           attach_retry_details(error, classification, attempt, policy, Enum.reverse(delays))}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp call(fun) do
    fun.()
  rescue
    error ->
      {:error,
       %Error{
         type: :execution_error,
         message: Exception.message(error),
         details: %{exception: error.__struct__},
         cause: error
       }}
  catch
    :exit, reason ->
      {:error,
       %Error{
         type: :execution_error,
         message: "SQL retry operation exited",
         details: %{reason: inspect(reason)},
         cause: reason
       }}
  end

  defp retry?(%Classification{} = classification, attempt, %Policy{} = policy) do
    Classification.retryable_in_phase?(classification) and attempt < policy.max_attempts
  end

  defp emit_retry_attempt(%Classification{} = classification, attempt, delay) do
    Observability.emit(
      [:retry, :attempt],
      %{attempt: attempt, delay_ms: delay},
      %{
        phase: classification.phase,
        class: classification.class,
        reason: classification.reason,
        sqlstate: classification.sqlstate
      }
    )
  end

  defp attach_retry_details(
         %Error{} = error,
         %Classification{} = classification,
         attempts,
         %Policy{} = policy,
         delays
       ) do
    retry_details = %{
      attempts: attempts,
      max_attempts: policy.max_attempts,
      delays_ms: delays,
      classification: Map.from_struct(classification)
    }

    %Error{error | details: Map.put(error.details || %{}, :retry, retry_details)}
  end
end
