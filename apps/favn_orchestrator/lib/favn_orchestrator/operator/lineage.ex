defmodule FavnOrchestrator.Operator.Lineage do
  @moduledoc """
  Public same-BEAM facade for bounded operator asset lineage views.

  Each query projects one pinned manifest version with current target statuses.
  Browser-facing callers receive explicit DTOs and never access persistence or
  manifest internals directly.
  """

  alias FavnOrchestrator.Operator.Lineage.AssetInspector
  alias FavnOrchestrator.Operator.Lineage.AssetNode
  alias FavnOrchestrator.Operator.Lineage.EdgeInspector
  alias FavnOrchestrator.Operator.Lineage.Error
  alias FavnOrchestrator.Operator.Lineage.Graph
  alias FavnOrchestrator.Operator.Lineage.GroupInspector
  alias FavnOrchestrator.Operator.Lineage.Projection
  alias FavnOrchestrator.Operator.Lineage.Query
  alias FavnOrchestrator.Operator.Lineage.Request
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Redaction

  @max_search_bytes 512
  @max_id_bytes 1_024

  @type error :: Error.t()
  @type graph_opts :: keyword()

  @doc "Returns a bounded grouped lineage graph for one manifest version."
  @spec get_graph(WorkspaceContext.t(), graph_opts()) :: {:ok, Graph.t()} | {:error, error()}
  def get_graph(%WorkspaceContext{} = context, opts \\ []) do
    with {:ok, model} <- read_model(context, opts), do: {:ok, model.graph}
  end

  @doc "Returns the inspector payload for one lineage group."
  @spec get_group(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, GroupInspector.t()} | {:error, error()}
  def get_group(%WorkspaceContext{} = context, group_id, opts \\ []) do
    with :ok <- validate_id(group_id),
         {:ok, model} <- read_model(context, put_selected_id(opts, group_id)),
         do: Query.group(model, group_id)
  end

  @doc "Returns the inspector payload for one lineage asset."
  @spec get_asset(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, AssetInspector.t()} | {:error, error()}
  def get_asset(%WorkspaceContext{} = context, asset_id, opts \\ []) do
    with :ok <- validate_id(asset_id),
         {:ok, model} <- read_model(context, put_selected_id(opts, asset_id)),
         do: Query.asset(model, asset_id)
  end

  @doc "Returns the inspector payload for one lineage dependency edge."
  @spec get_edge(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, EdgeInspector.t()} | {:error, error()}
  def get_edge(%WorkspaceContext{} = context, edge_id, opts \\ []) do
    with :ok <- validate_id(edge_id),
         {:ok, model} <- read_model(context, put_selected_id(opts, edge_id)),
         do: Query.edge(model, edge_id)
  end

  @doc "Searches lineage groups, schemas, and assets with bounded offset pagination."
  @spec search(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Page.t(term())} | {:error, error()}
  def search(%WorkspaceContext{} = context, query, opts \\ []) do
    with :ok <- validate_query(query),
         {:ok, model} <- read_model(context, opts),
         {:ok, page} <- Query.search(model, query, opts) do
      {:ok, page}
    else
      {:error, :invalid_pagination} -> {:error, invalid_pagination()}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @doc "Lists assets belonging to one lineage group with bounded offset pagination."
  @spec list_group_assets(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Page.t(AssetNode.t())} | {:error, error()}
  def list_group_assets(%WorkspaceContext{} = context, group_id, opts \\ []) do
    with :ok <- validate_id(group_id),
         {:ok, model} <- read_model(context, opts),
         {:ok, page} <- Query.group_assets(model, group_id, opts) do
      {:ok, page}
    else
      {:error, :invalid_pagination} -> {:error, invalid_pagination()}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp read_model(context, opts) do
    with {:ok, request} <- Request.normalize(opts),
         {:ok, model} <- Projection.read(context, request) do
      {:ok, model}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp validate_id(value)
       when is_binary(value) and byte_size(value) > 0 and byte_size(value) <= @max_id_bytes,
       do: :ok

  defp validate_id(_value), do: {:error, invalid_request()}

  defp validate_query(value) when is_binary(value) and byte_size(value) <= @max_search_bytes,
    do: :ok

  defp validate_query(_value), do: {:error, invalid_request()}

  defp put_selected_id(opts, id) when is_list(opts) do
    if Keyword.keyword?(opts), do: Keyword.put(opts, :selected_id, id), else: opts
  end

  defp put_selected_id(opts, _id), do: opts

  defp invalid_request,
    do: %Error{code: :invalid_request, message: "Invalid lineage request."}

  defp invalid_pagination,
    do: %Error{code: :invalid_request, message: "Invalid lineage pagination."}

  defp normalize_error({:storage_failed, reason}) do
    %Error{
      code: :storage_unavailable,
      message: "Lineage storage is unavailable.",
      retryable?: true,
      details: safe_details(reason)
    }
  end

  defp normalize_error(reason) do
    %Error{
      code: :lineage_projection_unavailable,
      message: "Lineage projection is unavailable.",
      retryable?: true,
      details: safe_details(reason)
    }
  end

  defp safe_details(reason) do
    %{reason: reason}
    |> Redaction.redact_operational_bounded()
    |> Map.take([:reason])
  end
end
