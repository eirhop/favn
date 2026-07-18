defmodule FavnOrchestrator.API.ExecutionPackagesRouter do
  @moduledoc false

  use Plug.Router

  require Logger

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Serializer
  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.ManifestPublication.Config
  alias FavnOrchestrator.API.Response
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.Persistence.Error

  @max_packages_per_request 100
  @max_package_bytes 4 * 1024 * 1024
  @max_package_batch_bytes 32 * 1024 * 1024

  plug(:match)
  plug(:dispatch)

  post "/missing" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, context} <- Authentication.platform_context(conn, :platform_operator),
         {:ok, config} <- Config.from_app_env(),
         {:ok, hashes} <- fetch_list(conn.body_params, "hashes"),
         {:ok, missing} <- ExecutionPackages.missing_hashes(context, hashes) do
      Response.data(conn, 200, %{
        missing: missing,
        publication_limits: %{
          max_packages: @max_packages_per_request,
          compressed_limit_bytes: config.compressed_limit_bytes,
          decompressed_limit_bytes: min(config.decompressed_limit_bytes, @max_package_batch_bytes)
        }
      })
    else
      {:error, :too_many_execution_package_hashes} ->
        validation_error(conn, "Too many execution package hashes")

      {:error, :invalid_execution_package_hash} ->
        validation_error(conn, "Invalid execution package hash")

      {:error, :invalid_execution_package} ->
        validation_error(conn, "Execution package hashes must be a JSON list")

      {:error, {:missing_field, field}} ->
        Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        Response.error(conn, 403, "forbidden", "Service cannot publish execution packages")

      {:error, %Error{kind: :invalid}} ->
        validation_error(conn, "Invalid execution package hash")

      {:error, reason} ->
        Logger.error("execution_package.missing failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  post "/" do
    with :ok <- Authentication.ensure_service(conn),
         {:ok, context} <- Authentication.platform_context(conn, :platform_operator),
         {:ok, values} <- fetch_list(conn.body_params, "packages"),
         :ok <- validate_package_count(values),
         {:ok, packages} <- decode_packages(values),
         :ok <- ExecutionPackages.register(context, packages) do
      Response.data(conn, 201, %{stored: length(packages)})
    else
      {:error, :too_many_execution_packages} ->
        validation_error(conn, "Too many execution packages")

      {:error, :execution_package_too_large} ->
        validation_error(conn, "Execution package exceeds the per-package size limit")

      {:error, :execution_package_batch_too_large} ->
        validation_error(conn, "Execution package batch exceeds the 32 MiB size limit")

      {:error, {:missing_field, field}} ->
        Response.error(conn, 422, "validation_failed", "Missing required field", %{field: field})

      {:error, :service_unauthorized} ->
        Response.error(conn, 401, "service_unauthorized", "Invalid service credentials")

      {:error, :forbidden} ->
        Response.error(conn, 403, "forbidden", "Service cannot publish execution packages")

      {:error, %Error{kind: :invalid}} ->
        validation_error(conn, "Invalid execution package")

      {:error, %Error{kind: :conflict}} ->
        Response.error(
          conn,
          409,
          "execution_package_conflict",
          "Execution package conflicts with stored content"
        )

      {:error, reason}
      when reason in [
             :invalid_execution_package,
             :duplicate_execution_package,
             :invalid_execution_package_hash
           ] ->
        validation_error(conn, "Invalid execution package")

      {:error, {tag, _value}} when tag in [:invalid_execution_package_hash] ->
        validation_error(conn, "Invalid execution package")

      {:error, {tag, _expected, _actual}}
      when tag in [
             :execution_package_hash_mismatch,
             :unsupported_execution_package_schema
           ] ->
        validation_error(conn, "Invalid execution package")

      {:error, reason} ->
        Logger.error("execution_package.put failed: #{inspect(reason)}")
        Response.error(conn, 400, "bad_request", "Request failed")
    end
  end

  match _ do
    Response.error(conn, 404, "not_found", "Route was not found")
  end

  defp fetch_list(params, field) when is_map(params) do
    case Map.fetch(params, field) do
      {:ok, values} when is_list(values) -> {:ok, values}
      {:ok, _value} -> {:error, :invalid_execution_package}
      :error -> {:error, {:missing_field, field}}
    end
  end

  defp validate_package_count(values) when length(values) <= @max_packages_per_request, do: :ok
  defp validate_package_count(_values), do: {:error, :too_many_execution_packages}

  defp decode_packages(values) do
    Enum.reduce_while(values, {:ok, [], 0}, fn value, {:ok, packages, total_bytes} ->
      with {:ok, encoded} <- Serializer.encode_manifest(value),
           :ok <- validate_package_size(encoded),
           :ok <- validate_package_batch_size(total_bytes, encoded),
           {:ok, package} <- ExecutionPackage.from_published(value) do
        {:cont, {:ok, [package | packages], total_bytes + byte_size(encoded)}}
      else
        {:error, :execution_package_too_large} = error -> {:halt, error}
        {:error, :execution_package_batch_too_large} = error -> {:halt, error}
        {:error, _reason} -> {:halt, {:error, :invalid_execution_package}}
      end
    end)
    |> case do
      {:ok, packages, _total_bytes} -> {:ok, Enum.reverse(packages)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_package_size(encoded) when byte_size(encoded) <= @max_package_bytes, do: :ok
  defp validate_package_size(_encoded), do: {:error, :execution_package_too_large}

  defp validate_package_batch_size(total_bytes, encoded)
       when total_bytes + byte_size(encoded) <= @max_package_batch_bytes,
       do: :ok

  defp validate_package_batch_size(_total_bytes, _encoded),
    do: {:error, :execution_package_batch_too_large}

  defp validation_error(conn, message) do
    Response.error(conn, 422, "validation_failed", message)
  end
end
