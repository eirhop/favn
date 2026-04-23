defmodule Favn.SQL.RuntimeBridge do
  @moduledoc false

  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan

  @spec connect(atom(), keyword()) :: {:ok, Session.t()} | {:error, term()}
  defdelegate connect(connection, opts \\ []), to: Client

  @spec disconnect(Session.t()) :: :ok
  defdelegate disconnect(session), to: Client

  @spec query(Session.t(), iodata(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate query(session, statement, opts), to: Client

  @spec execute(Session.t(), iodata(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate execute(session, statement, opts), to: Client

  @spec materialize(Session.t(), WritePlan.t(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate materialize(session, write_plan, opts), to: Client

  @spec get_relation(Session.t(), RelationRef.t()) :: {:ok, term()} | {:error, term()}
  def get_relation(session, relation_ref) do
    case Client.relation(session, relation_ref) do
      {:error, %Favn.SQL.Error{operation: :relation} = error} ->
        {:error, %Favn.SQL.Error{error | operation: :get_relation}}

      other ->
        other
    end
  end

  @spec columns(Session.t(), RelationRef.t()) :: {:ok, [term()]} | {:error, term()}
  defdelegate columns(session, relation_ref), to: Client

  @spec capabilities(Session.t()) :: {:ok, Favn.SQL.Capabilities.t()} | {:error, term()}
  defdelegate capabilities(session), to: Client

  @spec transaction(Session.t(), (Session.t() -> {:ok, term()} | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()}
  defdelegate transaction(session, fun, opts \\ []), to: Client
end
