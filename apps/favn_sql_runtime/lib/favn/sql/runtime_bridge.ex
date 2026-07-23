defmodule Favn.SQL.RuntimeBridge do
  @moduledoc false

  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.GenerationActivation
  alias Favn.SQL.GenerationDiscard
  alias Favn.SQL.GenerationMarkerInitialization
  alias Favn.SQL.GenerationReconciliation
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan

  @runner_registry FavnRunner.ConnectionRegistry

  @spec connect(atom(), keyword()) :: {:ok, Session.t()} | {:error, term()}
  def connect(connection, opts \\ [])

  def connect(connection, opts) when is_atom(connection) and is_list(opts) do
    Client.connect(connection, Keyword.put_new(opts, :registry_name, @runner_registry))
  end

  def connect(connection, opts), do: Client.connect(connection, opts)

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

  @spec generation_capabilities(Session.t(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate generation_capabilities(session, opts \\ []), to: Client

  @spec inspect_generation(Session.t(), RelationRef.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  defdelegate inspect_generation(session, relation_ref, opts \\ []), to: Client

  @spec initialize_generation_marker(
          Session.t(),
          GenerationMarkerInitialization.t(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  defdelegate initialize_generation_marker(session, request, opts \\ []), to: Client

  @spec activate_generation(Session.t(), GenerationActivation.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  defdelegate activate_generation(session, request, opts \\ []), to: Client

  @spec reconcile_generation(Session.t(), GenerationReconciliation.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  defdelegate reconcile_generation(session, request, opts \\ []), to: Client

  @spec discard_generation(Session.t(), GenerationDiscard.t(), keyword()) ::
          {:ok, :discarded} | {:error, term()}
  defdelegate discard_generation(session, request, opts \\ []), to: Client

  @spec transaction(Session.t(), (Session.t() -> {:ok, term()} | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()}
  defdelegate transaction(session, fun, opts \\ []), to: Client
end
