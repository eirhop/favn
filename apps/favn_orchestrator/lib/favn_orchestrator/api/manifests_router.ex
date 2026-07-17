defmodule FavnOrchestrator.API.ManifestsRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias Favn.Manifest.Version
  alias FavnOrchestrator
  alias FavnOrchestrator.API.Audit
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.DTO
  alias FavnOrchestrator.API.Filters
  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.API.Response

  plug(:match)
  plug(:dispatch)

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, manifests} <- FavnOrchestrator.list_manifest_summaries() do
      Response.data(conn, 200, %{items: manifests})
    else
      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, reason} ->
        Logger.error("manifest.list failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, version} <- build_version(conn.body_params),
         {:ok, registration_status, canonical_version} <- publish(version),
         {:ok, summary} <-
           FavnOrchestrator.get_manifest_summary(canonical_version.manifest_version_id) do
      Audit.put_best_effort(%{
        action: "manifest.register",
        session_id: nil,
        resource_type: "manifest",
        resource_id: canonical_version.manifest_version_id,
        outcome: "accepted",
        service_identity: Authentication.service_identity(conn)
      })

      Response.data(conn, publish_status(registration_status), %{
        manifest: summary,
        registration: %{
          status: Atom.to_string(registration_status),
          manifest_version_id: version.manifest_version_id,
          canonical_manifest_version_id: canonical_version.manifest_version_id
        }
      })
    else
      {:error, {:missing_field, field}} ->
        Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, {:invalid_manifest_version_id, _value}} ->
        validation_error(conn, "Invalid manifest version id")

      {:error, {:invalid_content_hash, _value}} ->
        validation_error(conn, "Invalid manifest content hash")

      {:error, {:manifest_content_hash_mismatch, _expected, _computed}} ->
        validation_error(conn, "Manifest content hash does not match payload")

      {:error, {:manifest_schema_version_mismatch, _expected, _actual}} ->
        validation_error(conn, "Manifest schema version does not match payload")

      {:error, {:manifest_runner_contract_version_mismatch, _expected, _actual}} ->
        validation_error(conn, "Manifest runner contract version does not match payload")

      {:error, :manifest_version_conflict} ->
        Response.error(
          conn,
          409,
          "manifest_conflict",
          "Manifest version id already exists with different content"
        )

      {:error, {:missing_execution_packages, missing}} ->
        Response.error(
          conn,
          422,
          "missing_execution_packages",
          "Manifest index references execution packages that have not been uploaded",
          %{hashes: missing}
        )

      {:error, {:execution_package_asset_mismatch, hash, _expected, _actual}} ->
        Response.error(
          conn,
          422,
          "execution_package_asset_mismatch",
          "Manifest index assigns an execution package to the wrong asset",
          %{hash: hash}
        )

      {:error, :service_unauthorized} ->
        authentication_error(conn, :service_unauthorized)

      {:error, reason} ->
        Logger.error("manifest.register failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/active" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, manifest_version_id} <- FavnOrchestrator.active_manifest(),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(manifest_version_id),
         {:ok, targets} <- FavnOrchestrator.manifest_targets(manifest_version_id) do
      Response.data(conn, 200, %{manifest: summary, targets: DTO.manifest_targets(targets)})
    else
      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} ->
        authentication_error(conn, reason)
    end
  end

  get "/:manifest_version_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, summary} <- FavnOrchestrator.get_manifest_summary(manifest_version_id),
         {:ok, targets} <- FavnOrchestrator.manifest_targets(manifest_version_id) do
      Response.data(conn, 200, %{manifest: summary, targets: DTO.manifest_targets(targets)})
    else
      {:error, :manifest_version_not_found} ->
        Response.error(conn, 404, "not_found", "Manifest version was not found")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, _reason} ->
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/:manifest_version_id/assets/:target_id/inspection" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor} <- Authentication.actor_context(conn, :viewer),
         {:ok, sample_limit} <- Filters.inspection_sample_limit(conn.params),
         {:ok, result} <-
           FavnOrchestrator.inspect_manifest_asset(manifest_version_id, target_id,
             sample_limit: sample_limit
           ) do
      Response.data(conn, 200, %{inspection: DTO.inspection_result(result)})
    else
      {:error, :manifest_version_not_found} ->
        Response.error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :invalid_asset_target} ->
        Response.error(conn, 404, "not_found", "Asset target was not found")

      {:error, reason}
      when reason in [
             :asset_not_found,
             :asset_relation_not_found,
             :relation_connection_missing,
             :invalid_relation,
             :invalid_inspection_target
           ] ->
        Response.error(conn, 422, "validation_failed", "Asset relation is not inspectable", %{
          reason: Atom.to_string(reason)
        })

      {:error, :invalid_sample_limit} ->
        validation_error(conn, "Invalid sample limit")

      {:error, :runner_client_not_available} ->
        Response.error(conn, 503, "service_unavailable", "Runner inspection is not available")

      {:error, reason} when reason in [:forbidden, :service_unauthorized, :unauthenticated] ->
        authentication_error(conn, reason)

      {:error, reason} ->
        Logger.error("inspection failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/:manifest_version_id/activate" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, session, actor} <- Authentication.actor_context(conn, :operator) do
      IdempotentCommand.run(
        conn,
        "manifest.activate",
        actor.id,
        session.id,
        %{manifest_version_id: manifest_version_id},
        fn idempotency ->
          activate(conn, manifest_version_id, session, actor, idempotency)
        end
      )
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:manifest_version_id/runner/register" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, registration} <-
           FavnOrchestrator.register_manifest_with_runner(manifest_version_id) do
      Response.data(conn, 200, %{registration: registration})
    else
      {:error, :manifest_version_not_found} ->
        Response.error(conn, 404, "not_found", "Manifest version was not found")

      {:error, :runner_manifest_conflict} ->
        Response.error(
          conn,
          409,
          "runner_manifest_conflict",
          "Runner has a different manifest for this version id"
        )

      {:error, reason} when reason in [:runner_client_not_available, :runner_unavailable] ->
        Response.error(
          conn,
          503,
          "runner_unavailable",
          "Runner manifest registration is unavailable"
        )

      {:error, :service_unauthorized} ->
        authentication_error(conn, :service_unauthorized)

      {:error, reason} ->
        Logger.error("runner manifest registration failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp activate(conn, manifest_version_id, session, actor, idempotency) do
    case FavnOrchestrator.activate_manifest(manifest_version_id) do
      :ok ->
        reload_scheduler_best_effort()

        %{
          action: "manifest.activate",
          actor_id: actor.id,
          session_id: session.id,
          resource_type: "manifest",
          resource_id: manifest_version_id,
          outcome: "accepted",
          service_identity: Authentication.service_identity(conn)
        }
        |> Map.merge(IdempotentCommand.audit_metadata(idempotency, "accepted"))
        |> Audit.put_best_effort()

        {:ok, 200, %{activated: true, manifest_version_id: manifest_version_id}, "manifest",
         manifest_version_id}

      {:error, :manifest_version_not_found} ->
        {:error, 404, "not_found", "Manifest version was not found", %{}}

      {:error, _reason} ->
        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  defp reload_scheduler_best_effort do
    case FavnOrchestrator.reload_scheduler() do
      :ok ->
        :ok

      {:error, :scheduler_not_running} ->
        :ok

      {:error, reason} ->
        Logger.warning("manifest activated but scheduler reload failed: #{inspect(reason)}")
    end
  end

  defp build_version(params) do
    with %{} = manifest <- Map.get(params, "manifest"),
         {:ok, version} <- Version.from_published(manifest, version_options(params)) do
      {:ok, version}
    else
      {:error, _reason} = error -> error
      _missing_or_invalid -> {:error, {:missing_field, "manifest"}}
    end
  end

  defp publish(%Version{} = version) do
    case FavnOrchestrator.publish_manifest(version) do
      {:ok, :published, %Version{} = canonical} -> {:ok, :published, canonical}
      {:ok, :already_published, %Version{} = canonical} -> {:ok, :already_published, canonical}
      {:error, reason} -> {:error, reason}
    end
  end

  defp publish_status(:published), do: 201
  defp publish_status(:already_published), do: 200

  defp version_options(params) do
    []
    |> put_option(:manifest_version_id, Map.get(params, "manifest_version_id"))
    |> put_option(:content_hash, Map.get(params, "content_hash"))
    |> put_option(:schema_version, Map.get(params, "schema_version"))
    |> put_option(:runner_contract_version, Map.get(params, "runner_contract_version"))
    |> put_option(:serialization_format, Map.get(params, "serialization_format"))
  end

  defp put_option(opts, key, value) when is_integer(value), do: Keyword.put(opts, key, value)

  defp put_option(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_option(opts, _key, _value), do: opts

  defp authentication_error(conn, :forbidden),
    do: Response.error(conn, 403, "forbidden", "Actor does not have access")

  defp authentication_error(conn, :service_unauthorized),
    do: Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

  defp authentication_error(conn, _reason),
    do: Response.error(conn, 401, "unauthenticated", "Missing or invalid actor context")

  defp validation_error(conn, message),
    do: Response.error(conn, 422, "validation_failed", message)
end
