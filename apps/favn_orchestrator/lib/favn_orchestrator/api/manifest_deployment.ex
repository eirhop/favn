defmodule FavnOrchestrator.API.ManifestDeployment do
  @moduledoc """
  Streams first-party manifest archives before the general JSON parser.

  Authentication, replay checks, and distributed upload admission all happen
  before the request body is read.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  alias Favn.Manifest.ArchiveLimits
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.ManifestDeploymentArchive
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.ManifestDeployments
  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestMemory.Slot
  alias FavnOrchestrator.ManifestUploadHeartbeat
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.ManifestDeployment, as: DeploymentResult
  alias FavnOrchestrator.RuntimeConfig

  @operation_pattern ~r/\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
  @sha_pattern ~r/\A[0-9a-f]{64}\z/
  @impl true
  def init(opts), do: opts

  @impl true
  def call(
        %Plug.Conn{path_info: ["api", "orchestrator", "v1", "manifest-deployments", operation_id]} =
          conn,
        opts
      ) do
    case conn.method do
      "PUT" -> put_archive(conn, operation_id, opts)
      "GET" -> get_status(conn, operation_id)
      _other -> conn
    end
  end

  def call(conn, _opts), do: conn

  defp put_archive(conn, operation_id, opts) do
    with :ok <- validate_operation_id(operation_id),
         {:ok, context} <- Authentication.manifest_deployment_context(conn),
         {:ok, archive_sha256} <- archive_sha256(conn),
         :ok <- validate_transport(conn),
         {:ok, preflight} <- ManifestDeployments.preflight(context, operation_id, archive_sha256) do
      case preflight do
        {:replay, operation} -> send_operation(conn, 200, operation)
        :new -> admit_and_upload(conn, context, operation_id, archive_sha256, opts)
      end
    else
      {:error, :invalid_operation_id} ->
        error(conn, 422, "validation_failed", "Manifest deployment operation id is invalid")

      {:error, :missing_archive_sha256} ->
        error(conn, 422, "validation_failed", "X-Favn-Archive-Sha256 is required")

      {:error, :invalid_archive_sha256} ->
        error(conn, 422, "validation_failed", "X-Favn-Archive-Sha256 is invalid")

      {:error, :unsupported_media_type} ->
        error(conn, 415, "unsupported_media_type", "Content-Type must be application/gzip")

      {:error, :unsupported_content_encoding} ->
        error(conn, 415, "unsupported_content_encoding", "Content-Encoding is not supported")

      {:error, :compressed_limit_exceeded} ->
        error(conn, 413, "manifest_archive_too_large", "Manifest archive exceeds 256 MiB")

      {:error, :invalid_content_length} ->
        error(conn, 400, "invalid_content_length", "Content-Length is invalid")

      {:error, :deployment_operation_conflict} ->
        error(
          conn,
          409,
          "deployment_operation_conflict",
          "Operation id already names different manifest content"
        )

      {:error, reason} ->
        authentication_or_infrastructure_error(conn, reason)
    end
  end

  defp admit_and_upload(conn, context, operation_id, archive_sha256, opts) do
    lease_id =
      "upload:#{RuntimeConfig.instance_id()}:#{operation_id}:#{System.unique_integer([:positive])}"

    case ManifestDeployments.acquire_upload(context, lease_id) do
      :ok ->
        heartbeat =
          ManifestUploadHeartbeat.start(fn ->
            ManifestDeployments.renew_upload(context, lease_id)
          end)

        try do
          case ManifestMemory.with_phase(
                 :upload,
                 fn ->
                   started_at_ms = System.monotonic_time(:millisecond)
                   deadline_ms = started_at_ms + ArchiveLimits.current().upload_timeout_ms

                   receive_archive(
                     conn,
                     context,
                     lease_id,
                     operation_id,
                     archive_sha256,
                     heartbeat,
                     started_at_ms,
                     deadline_ms,
                     opts
                   )
                 end,
                 memory_opts(opts)
               ) do
            %Plug.Conn{} = conn -> conn
            {:error, reason} -> memory_capacity_error(conn, reason)
          end
        after
          :ok = ManifestUploadHeartbeat.stop(heartbeat)
          _ = ManifestDeployments.release_upload(context, lease_id)
        end

      {:error, %Error{kind: :limit_exceeded, details: %{reason: :deployment_upload_busy}}} ->
        conn
        |> put_resp_header("retry-after", "5")
        |> error(429, "deployment_upload_busy", "Manifest upload capacity is busy")

      {:error, reason} ->
        infrastructure_error(conn, reason)
    end
  end

  defp receive_archive(
         conn,
         context,
         lease_id,
         operation_id,
         expected_sha256,
         heartbeat,
         started_at_ms,
         deadline_ms,
         opts
       ) do
    capacity_check = Keyword.get(opts, :capacity_check, &ManifestMemory.ensure_headroom/0)

    parser =
      ManifestDeploymentArchive.new(
        fn packages ->
          with :ok <- ensure_upload_active(heartbeat, deadline_ms),
               :ok <- ManifestDeployments.register_packages(context, packages) do
            ensure_upload_active(heartbeat, deadline_ms)
          end
        end,
        started_at_ms: started_at_ms,
        capacity_check: capacity_check
      )

    try do
      with {:ok, parser, conn} <- read_archive(conn, parser),
           :ok <- ensure_upload_active(heartbeat, deadline_ms),
           :ok <- capacity_check.(),
           {:ok, parsed} <- ManifestDeploymentArchive.finish(parser),
           :ok <- ensure_upload_active(heartbeat, deadline_ms),
           :ok <- capacity_check.(),
           :ok <- verify_archive_sha(parsed.archive_sha256, expected_sha256),
           fingerprint <-
             ManifestDeployments.fingerprint(
               context,
               operation_id,
               expected_sha256,
               parsed.version
             ),
           {:ok, status, operation} <-
             ManifestDeployments.accept(
               context,
               operation_id,
               lease_id,
               expected_sha256,
               fingerprint,
               parsed.version
             ) do
        Logger.info(
          "manifest deployment upload accepted",
          workspace_id: context.workspace_id,
          operation_id: operation_id,
          service_identity: context.service_identity,
          manifest_version_id: operation.manifest_version_id,
          compressed_bytes: parsed.compressed_bytes,
          expanded_bytes: parsed.expanded_bytes,
          entry_count: parsed.entry_count,
          package_count: parsed.package_count
        )

        send_operation(conn, if(status == :accepted, do: 202, else: 200), operation)
      else
        {:error, :archive_sha256_mismatch} ->
          error(
            conn,
            422,
            "manifest_archive_digest_mismatch",
            "Manifest archive SHA-256 does not match"
          )

        {:error, reason}
        when reason in [
               :compressed_limit_exceeded,
               :expanded_limit_exceeded,
               :bundle_limit_exceeded,
               :manifest_index_limit_exceeded,
               :execution_package_limit_exceeded,
               :execution_package_count_exceeded,
               :tar_entry_limit_exceeded,
               :manifest_memory_budget_exceeded
             ] ->
          error(
            conn,
            413,
            "manifest_archive_too_large",
            "Manifest archive exceeds a protocol limit"
          )

        {:error, :upload_timeout} ->
          error(conn, 408, "manifest_upload_timeout", "Manifest archive upload timed out")

        {:error, {:upload_lease_lost, _reason}} ->
          error(conn, 409, "manifest_upload_lease_lost", "Manifest upload lease was lost")

        {:error, {:package_persistence_failed, :upload_timeout}} ->
          error(conn, 408, "manifest_upload_timeout", "Manifest archive upload timed out")

        {:error, {:package_persistence_failed, {:upload_lease_lost, _reason}}} ->
          error(conn, 409, "manifest_upload_lease_lost", "Manifest upload lease was lost")

        {:error, %Error{kind: :conflict, details: %{reason: :manifest_upload_lease_lost}}} ->
          error(conn, 409, "manifest_upload_lease_lost", "Manifest upload lease was lost")

        {:error, {:package_persistence_failed, %Error{} = reason}} ->
          infrastructure_error(conn, reason)

        {:error, :package_persistence_failed} ->
          infrastructure_error(conn, :package_persistence_failed)

        {:error, reason}
        when reason in [
               :manifest_capacity_unavailable,
               :memory_capacity_unknown,
               :manifest_worker_timeout,
               :manifest_worker_failed
             ] ->
          memory_capacity_error(conn, reason)

        {:error, %Error{} = reason} ->
          infrastructure_error(conn, reason)

        {:error, _reason} ->
          error(conn, 422, "invalid_manifest_archive", "Manifest archive is invalid")
      end
    catch
      :exit, _reason ->
        error(conn, 400, "invalid_request_body", "Manifest archive body failed")
    after
      ManifestDeploymentArchive.discard(parser)
    end
  end

  defp read_archive(conn, parser) do
    limits = ArchiveLimits.current()

    case read_body(conn,
           length: limits.read_chunk_bytes,
           read_length: limits.read_chunk_bytes,
           read_timeout: RuntimeConfig.http_server().request_timeout_ms
         ) do
      {:ok, bytes, conn} ->
        feed_archive(parser, bytes, fn parser -> {:ok, parser, conn} end)

      {:more, bytes, conn} ->
        feed_archive(parser, bytes, fn parser ->
          read_archive(conn, parser)
        end)

      {:error, :timeout} ->
        ManifestDeploymentArchive.discard(parser)
        {:error, :upload_timeout}

      {:error, _reason} ->
        ManifestDeploymentArchive.discard(parser)
        {:error, :invalid_request_body}
    end
  end

  defp feed_archive(parser, bytes, continue) do
    case ManifestDeploymentArchive.feed(parser, bytes) do
      {:ok, parser} ->
        continue.(parser)

      {:error, _reason} = error ->
        ManifestDeploymentArchive.discard(parser)
        error
    end
  end

  defp ensure_upload_active(heartbeat, deadline_ms) do
    if System.monotonic_time(:millisecond) > deadline_ms,
      do: {:error, :upload_timeout},
      else: ManifestUploadHeartbeat.check(heartbeat)
  end

  defp get_status(conn, operation_id) do
    with :ok <- validate_operation_id(operation_id),
         {:ok, context} <- Authentication.manifest_deployment_context(conn),
         {:ok, operation} <- ManifestDeployments.get(context, operation_id) do
      send_operation(conn, 200, operation)
    else
      {:error, :invalid_operation_id} ->
        error(conn, 422, "validation_failed", "Manifest deployment operation id is invalid")

      {:error, %Error{kind: :not_found}} ->
        error(conn, 404, "not_found", "Manifest deployment was not found")

      {:error, reason} ->
        authentication_or_infrastructure_error(conn, reason)
    end
  end

  defp validate_operation_id(value) do
    if is_binary(value) and byte_size(value) in 1..128 and Regex.match?(@operation_pattern, value),
      do: :ok,
      else: {:error, :invalid_operation_id}
  end

  defp archive_sha256(conn) do
    case get_req_header(conn, "x-favn-archive-sha256") do
      [sha] when byte_size(sha) == 64 ->
        if Regex.match?(@sha_pattern, sha),
          do: {:ok, sha},
          else: {:error, :invalid_archive_sha256}

      [] ->
        {:error, :missing_archive_sha256}

      _invalid ->
        {:error, :invalid_archive_sha256}
    end
  end

  defp validate_transport(conn) do
    with :ok <- validate_content_type(conn),
         :ok <- validate_content_encoding(conn),
         :ok <- validate_content_length(conn) do
      :ok
    end
  end

  defp validate_content_type(conn) do
    case get_req_header(conn, "content-type") do
      [value] ->
        if value |> String.split(";", parts: 2) |> hd() |> String.trim() |> String.downcase() ==
             "application/gzip",
           do: :ok,
           else: {:error, :unsupported_media_type}

      _other ->
        {:error, :unsupported_media_type}
    end
  end

  defp validate_content_encoding(conn) do
    case get_req_header(conn, "content-encoding") do
      [] -> :ok
      _present -> {:error, :unsupported_content_encoding}
    end
  end

  defp validate_content_length(conn) do
    case get_req_header(conn, "content-length") do
      [] ->
        :ok

      [value] ->
        case Integer.parse(value) do
          {size, ""} when size >= 0 and size <= 256 * 1_024 * 1_024 -> :ok
          {size, ""} when size > 256 * 1_024 * 1_024 -> {:error, :compressed_limit_exceeded}
          _invalid -> {:error, :invalid_content_length}
        end

      _ambiguous ->
        {:error, :invalid_content_length}
    end
  end

  defp verify_archive_sha(sha, sha), do: :ok
  defp verify_archive_sha(_actual, _expected), do: {:error, :archive_sha256_mismatch}

  defp send_operation(conn, status, %DeploymentResult{} = operation) do
    Response.data(conn, status, %{
      operation: %{
        operation_id: operation.operation_id,
        workspace_id: operation.workspace_id,
        state: Atom.to_string(operation.state),
        manifest_version_id: operation.manifest_version_id,
        manifest_content_hash: operation.manifest_content_hash,
        runner_releases: operation.runner_releases,
        deployment_id: operation.deployment_id,
        failure_class: operation.failure_class,
        activation_diagnostics: operation.activation_diagnostics,
        progress: %{
          inspection_completed: operation.inspection_completed,
          inspection_total: operation.inspection_total
        },
        accepted_at: operation.accepted_at,
        activating_at: operation.activating_at,
        terminal_at: operation.terminal_at,
        updated_at: operation.updated_at,
        status_url: status_url(operation.operation_id)
      }
    })
    |> halt()
  end

  defp status_url(operation_id),
    do: "/api/orchestrator/v1/manifest-deployments/" <> URI.encode(operation_id)

  defp authentication_or_infrastructure_error(conn, reason) do
    case reason do
      :service_unauthorized ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      :unauthenticated ->
        error(conn, 401, "service_unauthorized", "Invalid service credentials")

      :forbidden ->
        error(conn, 403, "forbidden", "Manifest deployment workspace is not allowed")

      %Error{kind: :forbidden} ->
        error(conn, 403, "forbidden", "Manifest deployment workspace is not allowed")

      other ->
        infrastructure_error(conn, other)
    end
  end

  defp infrastructure_error(conn, reason) do
    Logger.error("manifest deployment request unavailable",
      failure_class: failure_class(reason)
    )

    error(
      conn,
      503,
      "deployment_acceptance_unknown",
      "Manifest deployment status is temporarily unavailable"
    )
  end

  defp failure_class(%Error{kind: kind}), do: Atom.to_string(kind)
  defp failure_class(_reason), do: "unavailable"

  defp memory_opts(opts) do
    [
      slot: Keyword.get(opts, :manifest_slot, Slot),
      capacity_check: Keyword.get(opts, :capacity_check, &ManifestMemory.ensure_headroom/0)
    ]
  end

  defp memory_capacity_error(conn, :manifest_capacity_busy) do
    conn
    |> put_resp_header("retry-after", "5")
    |> error(429, "manifest_capacity_busy", "Manifest capacity is busy")
  end

  defp memory_capacity_error(conn, reason)
       when reason in [
              :manifest_capacity_unavailable,
              :memory_capacity_unknown,
              :manifest_worker_timeout,
              :manifest_worker_failed
            ] do
    conn
    |> put_resp_header("retry-after", "5")
    |> error(503, "manifest_capacity_unavailable", "Manifest capacity is unavailable")
  end

  defp error(conn, status, code, message) do
    conn |> Response.error(status, code, message) |> halt()
  end
end
