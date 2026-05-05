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
         {:ok, _schema} <- validate_schema(dto) do
      {:ok, Map.fetch!(dto, "body")}
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
        {:ok, @error_schema, error_to_dto(body)}
      else
        success_to_dto(operation, body)
      end
    end
  end

  defp success_to_dto("manifest.activate", body) when is_map(body) do
    {:ok, schema!("manifest.activate"),
     %{
       "activated" => boolean_field(body, "activated"),
       "manifest_version_id" => string_field(body, "manifest_version_id")
     }}
  end

  defp success_to_dto("run.cancel", body) when is_map(body) do
    {:ok, schema!("run.cancel"),
     %{
       "cancelled" => boolean_field(body, "cancelled"),
       "run_id" => string_field(body, "run_id")
     }}
  end

  defp success_to_dto(operation, body)
       when operation in ["run.submit", "run.rerun", "backfill.submit", "backfill.window.rerun"] and
              is_map(body) do
    {:ok, schema!(operation), %{"run" => JsonSafe.data(field(body, "run") || %{})}}
  end

  defp success_to_dto(operation, body),
    do: {:error, {:invalid_idempotency_response_body, operation, body}}

  defp error_to_dto(body) when is_map(body) do
    %{
      "code" => string_field(body, "code") || "bad_request",
      "message" => string_field(body, "message") || "Request failed",
      "details" => JsonSafe.data(field(body, "details") || %{})
    }
  end

  defp error_to_dto(_body) do
    %{"code" => "bad_request", "message" => "Request failed", "details" => %{}}
  end

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

  defp validate_schema(%{"operation" => operation, "response_schema" => @error_schema}) do
    with {:ok, _schema} <- success_schema(operation), do: {:ok, @error_schema}
  end

  defp validate_schema(%{"operation" => operation, "response_schema" => response_schema}) do
    with {:ok, expected} <- success_schema(operation) do
      if response_schema == expected do
        {:ok, response_schema}
      else
        {:error, {:idempotency_response_schema_mismatch, operation, response_schema, expected}}
      end
    end
  end

  defp success_schema(operation) do
    case Map.fetch(@success_schemas, operation) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unsupported_idempotency_operation, operation}}
    end
  end

  defp schema!(operation), do: Map.fetch!(@success_schemas, operation)

  defp field(body, key) when is_map(body) and is_binary(key) do
    Map.get(body, key) || Map.get(body, String.to_existing_atom(key))
  rescue
    ArgumentError -> Map.get(body, key)
  end

  defp string_field(body, key) do
    case field(body, key) do
      value when is_binary(value) -> value
      value when is_atom(value) -> Atom.to_string(value)
      value when is_integer(value) or is_float(value) or is_boolean(value) -> to_string(value)
      _value -> nil
    end
  end

  defp boolean_field(body, key) do
    case field(body, key) do
      value when is_boolean(value) -> value
      _value -> false
    end
  end
end
