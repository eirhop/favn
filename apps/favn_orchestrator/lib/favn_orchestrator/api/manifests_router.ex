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
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Redaction

  plug(:match)
  plug(:dispatch)

  get "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, manifests} <- list_manifests(conn) do
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
         {:ok, registration_status, canonical_version, platform_context} <- publish(conn, version),
         summary <- Manifests.summary(canonical_version) do
      Audit.put_best_effort(platform_context, %{
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

      {:error, {:invalid_required_runner_release_id, _value}} ->
        validation_error(conn, "Invalid required runner release id")

      {:error, {:manifest_required_runner_release_id_mismatch, _expected, _actual}} ->
        validation_error(conn, "Manifest runner release id does not match payload")

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

      {:error,
       %Error{
         kind: :invalid,
         details: %{reason: :missing_execution_packages, hashes: missing}
       }} ->
        Response.error(
          conn,
          422,
          "missing_execution_packages",
          "Manifest index references execution packages that have not been uploaded",
          %{hashes: missing}
        )

      {:error,
       %Error{
         kind: :invalid,
         details: %{reason: :execution_package_asset_mismatch, hash: hash}
       }} ->
        Response.error(
          conn,
          422,
          "execution_package_asset_mismatch",
          "Manifest index assigns an execution package to the wrong asset",
          %{hash: hash}
        )

      {:error, %Error{kind: :conflict}} ->
        Response.error(
          conn,
          409,
          "manifest_conflict",
          "Manifest version id already exists with different content"
        )

      {:error, :service_unauthorized} ->
        authentication_error(conn, :service_unauthorized)

      {:error, :forbidden} ->
        authentication_error(conn, :forbidden)

      {:error, reason} ->
        Logger.error("manifest.register failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  get "/active" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, details} <- active_details(conn) do
      Response.data(conn, 200, normalize_details(details))
    else
      {:error, :active_manifest_not_set} ->
        Response.error(conn, 404, "not_found", "Active manifest is not set")

      {:error, reason} ->
        authentication_error(conn, reason)
    end
  end

  get "/:manifest_version_id" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, details} <- manifest_details(conn, manifest_version_id) do
      Response.data(conn, 200, normalize_details(details))
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
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, sample_limit} <- Filters.inspection_sample_limit(conn.params),
         {:ok, result} <-
           FavnOrchestrator.inspect_manifest_asset(context, manifest_version_id, target_id,
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
         {:ok, session, actor, context} <- activation_context(conn),
         {:ok, platform_context} <- Authentication.platform_context(conn, :platform_operator) do
      run_activation(conn, manifest_version_id, session, actor, platform_context, context)
    else
      {:error, reason} -> authentication_error(conn, reason)
    end
  end

  post "/:manifest_version_id/runner/register" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :operator),
         {:ok, registration} <-
           FavnOrchestrator.register_manifest_with_runner(context, manifest_version_id) do
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

  defp activate(
         conn,
         manifest_version_id,
         session,
         actor,
         platform_context,
         context,
         idempotency
       ) do
    case activate_manifest(conn, platform_context, context, manifest_version_id, idempotency) do
      {:ok, runtime} ->
        %{
          action: "manifest.activate",
          actor_id: actor.id,
          session_id: session.id,
          resource_type: "manifest",
          resource_id: manifest_version_id,
          outcome: "accepted",
          workspace_id: runtime.workspace_id,
          service_identity: Authentication.service_identity(conn)
        }
        |> Map.merge(IdempotentCommand.audit_metadata(idempotency, "accepted"))
        |> then(&Audit.put_best_effort(context, &1))

        {:ok, 200,
         %{
           activated: true,
           manifest_version_id: manifest_version_id,
           deployment_id: runtime.deployment_id,
           revision: runtime.revision
         }, "manifest", manifest_version_id}

      {:error, :manifest_version_not_found} ->
        {:error, 404, "not_found", "Manifest version was not found", %{}}

      {:error, reason} ->
        Logger.error(
          "manifest.activate failed: #{inspect(Redaction.redact_operational_bounded(reason))}"
        )

        {:error, 400, "bad_request", "Request failed", %{}}
    end
  end

  @doc false
  @spec build_version(map()) :: {:ok, Version.t()} | {:error, term()}
  def build_version(params) when is_map(params) do
    with %{} = manifest <- Map.get(params, "manifest"),
         {:ok, version} <- Version.from_published(manifest, version_options(params)) do
      {:ok, version}
    else
      {:error, _reason} = error -> error
      _missing_or_invalid -> {:error, {:missing_field, "manifest"}}
    end
  end

  defp publish(conn, %Version{} = version) do
    with {:ok, context} <- Authentication.platform_context(conn, :platform_operator),
         result <- Manifests.publish(context, version) do
      case result do
        {:ok, :published, %Version{} = canonical} ->
          {:ok, :published, canonical, context}

        {:ok, :already_published, %Version{} = canonical} ->
          {:ok, :already_published, canonical, context}

        {:error, reason} ->
          {:error, reason}
      end
    else
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
    |> put_option(:required_runner_release_id, Map.get(params, "required_runner_release_id"))
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

  defp list_manifests(conn) do
    with {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer),
         {:ok, %{manifest: manifest}} <- Manifests.active(context) do
      {:ok, [manifest]}
    end
  end

  defp active_details(conn) do
    with {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer) do
      Manifests.active(context)
    end
  end

  defp manifest_details(conn, manifest_version_id) do
    with {:ok, _session, _actor, context} <- Authentication.workspace_context(conn, :viewer) do
      Manifests.get_active_release(context, manifest_version_id)
    end
  end

  defp activation_context(conn), do: Authentication.workspace_context(conn, :admin)

  defp run_activation(
         conn,
         manifest_version_id,
         session,
         actor,
         platform_context,
         context
       ) do
    request = %{
      manifest_version_id: manifest_version_id,
      selection: Map.get(conn.body_params, "selection"),
      configuration: Map.get(conn.body_params, "configuration", %{})
    }

    execute = fn idempotency ->
      activate(
        conn,
        manifest_version_id,
        session,
        actor,
        platform_context,
        context,
        idempotency
      )
    end

    IdempotentCommand.run(
      conn,
      context,
      "manifest.activate",
      actor.id,
      session.id,
      request,
      execute
    )
  end

  defp activate_manifest(conn, platform_context, context, manifest_version_id, idempotency) do
    with %{} = selection <- Map.get(conn.body_params, "selection"),
         %{} = configuration <- Map.get(conn.body_params, "configuration", %{}) do
      Manifests.deploy(platform_context, context, manifest_version_id, selection,
        deployment_id: "deployment:" <> idempotency.key_hash,
        configuration: configuration,
        idempotency: idempotency.command_idempotency
      )
    else
      _invalid -> {:error, :invalid_deployment_selection}
    end
  end

  defp normalize_details(%{manifest: manifest, targets: targets}) do
    %{manifest: manifest, targets: DTO.manifest_targets(targets)}
  end
end
