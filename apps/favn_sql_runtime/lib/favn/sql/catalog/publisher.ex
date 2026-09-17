defmodule Favn.SQL.Catalog.Publisher do
  @moduledoc """
  One-shot SQL catalog publication with fresh sessions and bounded reconciliation.

  The caller owns the connection registry. Only the selected catalog is admitted.
  Completed replay returns its original receipt. Uncertain native outcomes are
  never automatically resubmitted; reconciliation is read-only on a fresh session.
  """
  alias Favn.SQL.{Client, Deadline, Error}
  alias Favn.SQL.Catalog.Request

  @doc "Publishes, reconciles or rebuilds one validated request within the supplied overall deadline."
  @spec run(Request.t(), pid(), Deadline.t(), :publish | :reconcile | :rebuild, keyword()) ::
          {:ok, map()} | {:error, map()}
  def run(
        %Request{} = request,
        registry,
        %Deadline{} = deadline,
        mode \\ :publish,
        connect_options \\ []
      ) do
    result =
      session(request, registry, deadline, connect_options, fn session, backend ->
        if function_exported?(backend, mode, 3),
          do: apply(backend, mode, [session, request, deadline]),
          else: {:error, :unsupported_catalog_publication}
      end)

    case result do
      {:ok, receipt} ->
        {:ok, Map.put(receipt, "target", request.target)}

      {:error, %Error{type: :catalog_conflict}} ->
        observed =
          case session(request, registry, deadline, connect_options, fn session, backend ->
                 backend.observe(session, request, deadline)
               end) do
            {:ok, selected} -> selected
            _ -> nil
          end

        failure_result(request, :catalog_conflict, observed)

      {:error, reason} ->
        if mode == :rebuild and uncertain?(reason) do
          failure(request, :rebuild_outcome_unknown)
        else
          reconcile_failure(mode, reason, request, registry, deadline, connect_options)
        end
    end
  rescue
    _ -> failure(request, unknown(mode))
  catch
    :exit, _ -> failure(request, unknown(mode))
  end

  defp unknown(:rebuild), do: :rebuild_outcome_unknown
  defp unknown(_), do: :publication_outcome_unknown

  defp reconcile_failure(mode, reason, request, registry, deadline, connect_options) do
    if mode == :publish and uncertain?(reason) do
      case session(request, registry, deadline, connect_options, fn session, backend ->
             backend.reconcile(session, request, deadline)
           end) do
        {:ok, receipt} -> {:ok, Map.put(receipt, "target", request.target)}
        _ -> failure(request, :publication_outcome_unknown)
      end
    else
      failure(request, code(reason), reason)
    end
  end

  defp session(request, registry, deadline, connect_options, fun) do
    if Deadline.expired?(deadline) do
      {:error, :deadline_exceeded}
    else
      with {:ok, session} <-
             Client.connect(
               request.connection,
               connect_options ++
                 [
                   registry_name: registry,
                   pool: false,
                   required_catalogs: [request.catalog],
                   timeout_ms: Deadline.remaining_ms(deadline),
                   max_rows: 10001,
                   max_result_bytes: 134_217_728
                 ]
             ) do
        try do
          if function_exported?(session.adapter, :catalog_publication_backend, 0) do
            backend = session.adapter.catalog_publication_backend()
            with :ok <- backend.qualify(session, request, deadline), do: fun.(session, backend)
          else
            {:error, :unsupported_catalog_publication}
          end
        after
          Client.disconnect(session)
        end
      end
    end
  end

  defp uncertain?(%Error{type: :operation_timeout}), do: true

  defp uncertain?(%Error{details: details}),
    do:
      details[:transaction_stage] in [:commit, :rollback] or details[:unknown_outcome?] == true or
        details[:rollback_failed?] == true

  defp uncertain?(_), do: false

  defp code(%Error{type: type})
       when type in [:catalog_conflict, :catalog_integrity_failure, :catalog_schema_conflict, :catalog_rebuild_limit_exceeded],
       do: type

  defp code(reason) when is_atom(reason), do: reason
  defp code(_), do: :publication_failed

  defp failure(request, reason, error \\ nil) do
    observed =
      case error do
        %Error{type: :catalog_conflict, details: %{observed: observed}} -> observed
        _ -> nil
      end

    failure_result(request, reason, observed)
  end

  defp failure_result(request, reason, observed),
    do:
      {:error,
       %{
         "outcome" => "error",
         "reason" => to_string(reason),
         "target" => request.target,
         "operation_id" => request.operation_id,
         "artifacts" => Map.new(request.projections, &{&1.kind, &1.identity}),
         "expected" => request.expectations,
         "observed" => observed,
         "compatibility" => "unknown"
       }}
end
