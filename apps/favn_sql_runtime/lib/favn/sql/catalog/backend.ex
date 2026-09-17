defmodule Favn.SQL.Catalog.Backend do
  @moduledoc """
  Opt-in atomic catalog publication boundary for qualified SQL adapters.

  Implementations must qualify native metadata/macro DDL, conditional selection,
  receipt reconciliation, concurrent bootstrap and rollback. Generic transaction
  support does not imply this capability. No callback retries writes.
  """
  alias Favn.SQL.{Deadline, Session}
  alias Favn.SQL.Catalog.Request
  @callback applications() :: [atom()]
  @callback qualify(Session.t(), Request.t(), Deadline.t()) :: :ok | {:error, term()}
  @callback publish(Session.t(), Request.t(), Deadline.t()) :: {:ok, map()} | {:error, term()}
  @callback observe(Session.t(), Request.t(), Deadline.t()) :: {:ok, map()} | {:error, term()}
  @callback reconcile(Session.t(), Request.t(), Deadline.t()) :: {:ok, map()} | {:error, term()}
end
