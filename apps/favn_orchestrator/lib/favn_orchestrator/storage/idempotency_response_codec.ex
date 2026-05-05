defmodule FavnOrchestrator.Storage.IdempotencyResponseCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.idempotency_response.storage.v1"
  @schema_version 1
  @error_schema "favn.command.error.response.v1"

  @success_schemas %{
    "manifest.activate" => "favn.command.manifest_activate.response.v1",
    "run.submit" => "favn.command.run_submit.response.v1",
    "run.cancel" => "favn.command.run_cancel.response.v1",
    "run.rerun" => "favn.command.run_rerun.response.v1",
    "backfill.submit" => "favn.command.backfill_submit.response.v1",
    "backfill.window.rerun" => "favn.command.backfill_window_rerun.response.v1"
  }

  @type json_value :: map() | list() | String.t() | number() | boolean() | nil

  @spec encode(String.t(), term()) :: {:ok, String.t()} | {:error, term()}
  def encode(operation, body) when is_binary(operation) do
    with {:ok, response_schema, dto_body} <- body_to_dto(operation, body) do
      payload = %{
        "format" => @format,
        "schema_version" => @schema_version,
        "operation" => operation,
        "response_schema" => response_schema,
        "body" => dto_body
      }

      {:ok, Jason.encode!(payload)}
    end
  rescue
    error -> {:error, {:idempotency_response_encode_failed, error}}
  end

  def encode(operation, _body), do: {:error, {:invalid_idempotency_operation, operation}}

  @spec decode(String.t()) :: {:ok, json_value()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, decoded} <- Jason.decode(payload),
         {:ok, dto} <- validate_root(decoded),
         {:ok, response_schema} <- validate_schema(dto),
         {:ok, body} <-
           validate_body(Map.fetch!(dto, "operation"), response_schema, Map.fetch!(dto, "body")) do
      {:ok, body}
    else
      {:error, %Jason.DecodeError{} = reason} ->
        {:error, {:invalid_idempotency_response_json, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def decode(payload), do: {:error, {:invalid_idempotency_response_payload, payload}}

  defp body_to_dto(operation, body) do
    with {:ok, _success_schema} <- success_schema(operation) do
      if error_body?(body) do
        with {:ok, dto} <- error_to_dto(body), do: {:ok, @error_schema, dto}
      else
        success_to_dto(operation, body)
      end
    end
  end

  defp success_to_dto("manifest.activate", body) when is_map(body) do
    with {:ok, activated} <- required_boolean(body, "activated"),
         {:ok, manifest_version_id} <- required_string(body, "manifest_version_id") do
      {:ok, schema!("manifest.activate"),
       %{"activated" => activated, "manifest_version_id" => manifest_version_id}}
    else
      {:error, reason} ->
        {:error, {:invalid_idempotency_response_body, "manifest.activate", reason}}
    end
  end

  defp success_to_dto("run.cancel", body) when is_map(body) do
    with {:ok, cancelled} <- required_boolean(body, "cancelled"),
         {:ok, run_id} <- required_string(body, "run_id") do
      {:ok, schema!("run.cancel"), %{"cancelled" => cancelled, "run_id" => run_id}}
    else
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, "run.cancel", reason}}
    end
  end

  defp success_to_dto(operation, body)
       when operation in ["run.submit", "run.rerun", "backfill.submit", "backfill.window.rerun"] and
              is_map(body) do
    case required_map(body, "run") do
      {:ok, run} -> {:ok, schema!(operation), %{"run" => JsonSafe.data(run)}}
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp success_to_dto(operation, body),
    do: {:error, {:invalid_idempotency_response_body, operation, body}}

  defp error_to_dto(body) when is_map(body) do
    with {:ok, code} <- required_string(body, "code"),
         {:ok, message} <- required_string(body, "message"),
         {:ok, details} <- optional_map(body, "details", %{}) do
      {:ok, %{"code" => code, "message" => message, "details" => JsonSafe.data(details)}}
    else
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, :error, reason}}
    end
  end

  defp error_to_dto(body), do: {:error, {:invalid_idempotency_response_body, :error, body}}

  defp error_body?(body) when is_map(body) do
    not is_nil(field(body, "code")) and not is_nil(field(body, "message"))
  end

  defp error_body?(_body), do: false

  defp validate_root(
         %{
           "format" => @format,
           "schema_version" => @schema_version,
           "operation" => operation,
           "response_schema" => response_schema,
           "body" => _body
         } = dto
       )
       when is_binary(operation) and is_binary(response_schema) do
    {:ok, dto}
  end

  defp validate_root(%{"format" => format}) when format != @format,
    do: {:error, {:invalid_idempotency_response_format, format}}

  defp validate_root(%{"schema_version" => version}) when version != @schema_version,
    do: {:error, {:unsupported_idempotency_response_schema_version, version}}

  defp validate_root(dto), do: {:error, {:invalid_idempotency_response_dto, dto}}

  defp validate_schema(%{"operation" => operation, "response_schema" => response_schema}) do
    with {:ok, expected} <- success_schema(operation) do
      cond do
        response_schema == "favn.command.error.response.v1" ->
          {:ok, "favn.command.error.response.v1"}

        response_schema == expected ->
          {:ok, response_schema}

        true ->
          {:error, {:idempotency_response_schema_mismatch, operation, response_schema, expected}}
      end
    end
  end

  defp validate_body(
         "manifest.activate",
         "favn.command.manifest_activate.response.v1",
         body
       )
       when is_map(body) do
    with {:ok, _activated} <- required_boolean(body, "activated"),
         {:ok, _manifest_version_id} <- required_string(body, "manifest_version_id") do
      {:ok, body}
    else
      {:error, reason} ->
        {:error, {:invalid_idempotency_response_body, "manifest.activate", reason}}
    end
  end

  defp validate_body("run.cancel", "favn.command.run_cancel.response.v1", body)
       when is_map(body) do
    with {:ok, _cancelled} <- required_boolean(body, "cancelled"),
         {:ok, _run_id} <- required_string(body, "run_id") do
      {:ok, body}
    else
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, "run.cancel", reason}}
    end
  end

  defp validate_body(operation, @error_schema, body) when is_map(body) do
    with {:ok, _code} <- required_string(body, "code"),
         {:ok, _message} <- required_string(body, "message"),
         {:ok, _details} <- required_map(body, "details") do
      {:ok, body}
    else
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp validate_body(operation, schema, body)
       when operation in ["run.submit", "run.rerun", "backfill.submit", "backfill.window.rerun"] and
              is_map(body) do
    with {:ok, ^schema} <- success_schema(operation),
         {:ok, _run} <- required_map(body, "run") do
      {:ok, body}
    else
      {:ok, expected} ->
        {:error, {:idempotency_response_schema_mismatch, operation, schema, expected}}

      {:error, reason} ->
        {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp validate_body(operation, _schema, body),
    do: {:error, {:invalid_idempotency_response_body, operation, body}}

  defp success_schema(operation) do
    case Map.fetch(@success_schemas, operation) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unsupported_idempotency_operation, operation}}
    end
  end

  defp schema!(operation), do: Map.fetch!(@success_schemas, operation)

  defp field(body, key) when is_map(body) and is_binary(key) do
    if Map.has_key?(body, key) do
      Map.get(body, key)
    else
      Map.get(body, String.to_existing_atom(key))
    end
  rescue
    ArgumentError -> Map.get(body, key)
  end

  defp required_string(body, key) do
    case field(body, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value when is_atom(value) and not is_nil(value) -> {:ok, Atom.to_string(value)}
      value -> {:error, {:invalid_field, key, value}}
    end
  end

  defp required_boolean(body, key) do
    case field(body, key) do
      value when is_boolean(value) -> {:ok, value}
      value -> {:error, {:invalid_field, key, value}}
    end
  end

  defp required_map(body, key) do
    case field(body, key) do
      value when is_map(value) -> {:ok, value}
      value -> {:error, {:invalid_field, key, value}}
    end
  end

  defp optional_map(body, key, default) do
    case field(body, key) do
      nil -> {:ok, default}
      value when is_map(value) -> {:ok, value}
      value -> {:error, {:invalid_field, key, value}}
    end
  end
end
