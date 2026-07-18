defmodule FavnStoragePostgres.ErrorMapper do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Error

  @spec map(term()) :: Error.t()
  def map(%Error{} = error), do: error

  def map(%Postgrex.Error{postgres: %{code: :unique_violation, constraint: constraint}}) do
    Error.new(:conflict, "persistence identity already exists",
      details: %{constraint: constraint}
    )
  end

  def map(%Postgrex.Error{postgres: %{code: :foreign_key_violation, constraint: constraint}}) do
    Error.new(:constraint, "persistence relationship is invalid",
      details: %{constraint: constraint}
    )
  end

  def map(%Postgrex.Error{postgres: %{code: code, constraint: constraint}})
      when code in [:check_violation, :not_null_violation] do
    Error.new(:constraint, "persistence value violates an invariant",
      details: %{constraint: constraint}
    )
  end

  def map(%Postgrex.Error{postgres: %{code: code}})
      when code in [:serialization_failure, :deadlock_detected, :lock_not_available] do
    Error.new(:conflict, "transient database concurrency conflict", retryable?: true)
  end

  def map(%DBConnection.ConnectionError{}) do
    Error.new(:unavailable, "database connection unavailable", retryable?: true)
  end

  def map(%Postgrex.Error{postgres: %{code: :query_canceled}}) do
    Error.new(:timeout, "database statement exceeded its time budget", retryable?: true)
  end

  def map(%Ecto.ConstraintError{constraint: constraint}) do
    Error.new(:constraint, "persistence constraint rejected the operation",
      details: %{constraint: constraint}
    )
  end

  def map(:not_found), do: Error.new(:not_found, "persistence record not found")
  def map(:fenced), do: Error.new(:fenced, "persistence owner fencing token is stale")
  def map(:conflict), do: Error.new(:conflict, "persistence write conflicts with committed state")
  def map(:invalid), do: Error.new(:invalid, "invalid persistence command")

  def map(_reason), do: Error.new(:internal, "internal persistence failure")
end
