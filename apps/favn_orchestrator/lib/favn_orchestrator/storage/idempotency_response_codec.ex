defmodule FavnOrchestrator.Storage.IdempotencyResponseCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.idempotency_response.storage.v1"
  @schema_version 1
  @error_schema "favn.command.error.response.v1"
  @root_fields ~w(format schema_version operation response_schema body)

  @success_schemas %{
    "manifest.activate" => "favn.command.manifest_activate.response.v1",
    "run.submit" => "favn.command.run_submit.response.v1",
    "run.cancel" => "favn.command.run_cancel.response.v1",
    "run.rerun" => "favn.command.run_rerun.response.v1",
    "backfill.submit" => "favn.command.backfill_submit.response.v1",
    "backfill.window.rerun" => "favn.command.backfill_window_rerun.response.v1"
  }
  @simple_success_fields %{
    "manifest.activate" => [{"activated", :boolean}, {"manifest_version_id", :string}],
    "run.cancel" => [{"cancelled", :boolean}, {"run_id", :string}]
  }
  @run_operations ~w(run.submit run.rerun backfill.submit backfill.window.rerun)

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

  defp success_to_dto(operation, body) do
    with {:ok, schema} <- success_schema(operation),
         {:ok, dto} <- success_body(operation, body) do
      {:ok, schema, dto}
    else
      {:error, {:unsupported_idempotency_operation, _operation} = reason} -> {:error, reason}
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp error_to_dto(body) when is_map(body) do
    with {:ok, code} <- required_string(body, "code"),
         {:ok, message} <- required_string(body, "message"),
         {:ok, details} <- optional_map(body, "details", %{}) do
      {:ok, %{"code" => code, "message" => message, "details" => JsonSafe.data(details)}}
    else
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, :error, reason}}
    end
  end

  defp error_body(body) when is_map(body) do
    with {:ok, code} <- required_string(body, "code"),
         {:ok, message} <- required_string(body, "message"),
         {:ok, details} <- required_map(body, "details") do
      {:ok, %{"code" => code, "message" => message, "details" => JsonSafe.data(details)}}
    end
  end

  defp error_body(body), do: {:error, body}

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
    case dto |> Map.keys() |> Kernel.--(@root_fields) |> Enum.sort() do
      [] -> {:ok, dto}
      fields -> {:error, {:unknown_idempotency_response_fields, fields}}
    end
  end

  defp validate_root(%{"format" => format}) when format != @format,
    do: {:error, {:invalid_idempotency_response_format, format}}

  defp validate_root(%{"schema_version" => version}) when version != @schema_version,
    do: {:error, {:unsupported_idempotency_response_schema_version, version}}

  defp validate_root(dto), do: {:error, {:invalid_idempotency_response_dto, dto}}

  defp validate_schema(%{"operation" => operation, "response_schema" => response_schema}) do
    with {:ok, expected} <- success_schema(operation) do
      cond do
        response_schema == @error_schema ->
          {:ok, @error_schema}

        response_schema == expected ->
          {:ok, response_schema}

        true ->
          {:error, {:idempotency_response_schema_mismatch, operation, response_schema, expected}}
      end
    end
  end

  defp validate_body(operation, @error_schema, body) do
    case error_body(body) do
      {:ok, dto} -> {:ok, dto}
      {:error, reason} -> {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp validate_body(operation, schema, body) do
    with {:ok, ^schema} <- success_schema(operation),
         {:ok, dto} <- success_body(operation, body) do
      {:ok, dto}
    else
      {:ok, expected} ->
        {:error, {:idempotency_response_schema_mismatch, operation, schema, expected}}

      {:error, reason} ->
        {:error, {:invalid_idempotency_response_body, operation, reason}}
    end
  end

  defp success_schema(operation) do
    case Map.fetch(@success_schemas, operation) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unsupported_idempotency_operation, operation}}
    end
  end

  defp success_body(operation, body) when is_map_key(@simple_success_fields, operation) do
    @simple_success_fields
    |> Map.fetch!(operation)
    |> Enum.reduce_while({:ok, %{}}, fn {field, type}, {:ok, acc} ->
      case required_field(body, field, type) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, field, value)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp success_body(operation, body) when operation in @run_operations and is_map(body) do
    case required_map(body, "run") do
      {:ok, run} -> {:ok, %{"run" => JsonSafe.data(run)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp success_body(_operation, body), do: {:error, body}

  defp required_field(body, field, :boolean), do: required_boolean(body, field)
  defp required_field(body, field, :string), do: required_string(body, field)

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
