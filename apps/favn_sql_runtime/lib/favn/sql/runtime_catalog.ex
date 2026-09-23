defmodule Favn.SQL.RuntimeCatalog do
  @moduledoc """
  Adapter boundary for metadata written inside an existing asset transaction.

  Preparation rejects duplicate operations before mutation. Recording never
  commits or retries: the caller owns the transaction and its unknown outcome.
  """
  alias Favn.RuntimeCatalog.Publication
  alias Favn.SQL.{Error, Session}

  @callback resolve(Session.t(), Favn.RelationRef.t(), keyword()) ::
              {:ok, Favn.RelationRef.t()} | {:error, term()}

  @doc "Resolves a tracked target on its owner session before acquiring write admission."
  @spec resolve(Session.t(), Publication.t() | nil, Favn.RelationRef.t(), keyword()) ::
          {:ok, Favn.RelationRef.t()} | {:error, term()}
  def resolve(_, nil, relation, _), do: {:ok, relation}

  def resolve(session, %Publication{}, relation, opts) do
    with {:ok, backend} <- backend(session), do: backend.resolve(session, relation, opts)
  end

  @callback qualify_materialization_retry(
              Session.t(),
              Publication.t(),
              Favn.RelationRef.t(),
              keyword()
            ) ::
              {:ok, :supported | :unsupported} | {:error, Error.t()}
  @optional_callbacks qualify_materialization_retry: 4

  @doc "Checks native support for replay of a rejected ordinary managed transaction. Missing support fails closed."
  @spec qualify_materialization_retry(
          Session.t(),
          Publication.t() | nil,
          Favn.RelationRef.t(),
          keyword()
        ) ::
          {:ok, :supported | :unsupported} | {:error, Error.t()}
  def qualify_materialization_retry(
        session,
        %Publication{candidate: false} = publication,
        relation,
        opts
      ) do
    with {:ok, backend} <- backend(session) do
      if function_exported?(backend, :qualify_materialization_retry, 4),
        do:
          normalize_qualification(
            backend.qualify_materialization_retry(session, publication, relation, opts)
          ),
        else: {:ok, :unsupported}
    end
  end

  def qualify_materialization_retry(_, _, _, _), do: {:ok, :unsupported}

  @callback prepare(Session.t(), Publication.t(), Favn.RelationRef.t(), keyword()) ::
              {:ok, map()} | {:error, term()}
  @callback record(Session.t(), map(), map(), keyword()) :: {:ok, map()} | {:error, term()}

  @doc "Prepares the pinned publication on its owner-exclusive SQL session."
  @spec prepare(Session.t(), Publication.t() | nil, Favn.RelationRef.t(), keyword()) ::
          {:ok, map() | nil} | {:error, term()}
  def prepare(_session, nil, _relation, _opts), do: {:ok, nil}

  def prepare(%Session{} = session, %Publication{} = publication, relation, opts) do
    with :ok <- Publication.validate(publication), {:ok, backend} <- backend(session) do
      backend.prepare(session, publication, relation, opts)
    end
  end

  @doc "Records a successful mutation; a skipped write adds no receipt."
  @spec record(Session.t(), map() | nil, map(), keyword()) ::
          {:ok, map() | nil} | {:error, term()}
  def record(_, nil, _, _), do: {:ok, nil}
  def record(_, _, %{write_outcome: :no_op}, _), do: {:ok, nil}

  def record(session, prepared, output, opts) do
    with {:ok, backend} <- backend(session), do: backend.record(session, prepared, output, opts)
  end

  defp normalize_qualification({:ok, support}) when support in [:supported, :unsupported],
    do: {:ok, support}

  defp normalize_qualification({:error, %Error{}} = error), do: error

  defp normalize_qualification(_),
    do:
      {:error,
       %Error{
         type: :execution_error,
         message: "Invalid materialization retry qualification",
         retryable?: false
       }}

  defp backend(%Session{adapter: adapter}) do
    if function_exported?(adapter, :runtime_catalog_backend, 0),
      do: {:ok, adapter.runtime_catalog_backend()},
      else:
        {:error,
         %Error{
           type: :unsupported_runtime_catalog,
           message: "Runtime publication requires a qualified native adapter",
           retryable?: false
         }}
  end
end
