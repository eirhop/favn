defmodule FavnOrchestrator.Storage.AuthCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe

  @roles_format "favn.auth.roles.storage.v1"
  @credential_format "favn.auth.credential.storage.v1"
  @audit_format "favn.auth.audit.storage.v1"
  @schema_version 1

  @known_roles [:viewer, :operator, :admin]
  @audit_fields [:id, :occurred_at, :action, :actor_id, :session_id, :outcome, :service_identity]
  @audit_field_strings Enum.map(@audit_fields, &Atom.to_string/1)

  @spec encode_roles([atom()]) :: {:ok, String.t()} | {:error, term()}
  def encode_roles(roles) when is_list(roles) do
    with {:ok, role_strings} <- roles_to_dto(roles) do
      {:ok,
       Jason.encode!(%{
         "format" => @roles_format,
         "schema_version" => @schema_version,
         "roles" => role_strings
       })}
    end
  rescue
    error -> {:error, {:auth_roles_encode_failed, error}}
  end

  def encode_roles(value), do: {:error, {:invalid_auth_roles, value}}

  @spec decode_roles(String.t()) :: {:ok, [atom()]} | {:error, term()}
  def decode_roles(payload) when is_binary(payload) do
    with {:ok, %{"format" => @roles_format, "schema_version" => @schema_version} = dto} <-
           Jason.decode(payload),
         {:ok, roles} <- roles_from_dto(Map.get(dto, "roles")) do
      {:ok, roles}
    else
      {:ok, %{"format" => @roles_format, "schema_version" => version}} ->
        {:error, {:unsupported_auth_roles_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_auth_roles_dto, other}}

      {:error, %Jason.DecodeError{} = reason} ->
        {:error, {:invalid_auth_roles_json, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def decode_roles(value), do: {:error, {:invalid_auth_roles_payload, value}}

  @spec encode_credential(map()) :: {:ok, String.t()} | {:error, term()}
  def encode_credential(%{password_hash: "$argon2id$" <> _ = password_hash}) do
    {:ok,
     Jason.encode!(%{
       "format" => @credential_format,
       "schema_version" => @schema_version,
       "credential" => %{
         "kind" => "password_hash",
         "algorithm" => "argon2id",
         "password_hash" => password_hash
       }
     })}
  rescue
    error -> {:error, {:auth_credential_encode_failed, error}}
  end

  def encode_credential(value), do: {:error, {:invalid_auth_credential, value}}

  @spec decode_credential(String.t()) :: {:ok, map()} | {:error, term()}
  def decode_credential(payload) when is_binary(payload) do
    with {:ok, %{"format" => @credential_format, "schema_version" => @schema_version} = dto} <-
           Jason.decode(payload),
         {:ok, credential} <- credential_from_dto(Map.get(dto, "credential")) do
      {:ok, credential}
    else
      {:ok, %{"format" => @credential_format, "schema_version" => version}} ->
        {:error, {:unsupported_auth_credential_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_auth_credential_dto, other}}

      {:error, %Jason.DecodeError{} = reason} ->
        {:error, {:invalid_auth_credential_json, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def decode_credential(value), do: {:error, {:invalid_auth_credential_payload, value}}

  @spec encode_audit(map()) :: {:ok, String.t()} | {:error, term()}
  def encode_audit(entry) when is_map(entry) do
    with {:ok, id} <- required_binary(entry, :id),
         {:ok, occurred_at} <- required_datetime(entry, :occurred_at) do
      dto = %{
        "format" => @audit_format,
        "schema_version" => @schema_version,
        "id" => id,
        "occurred_at" => DateTime.to_iso8601(occurred_at),
        "action" => optional_binary(entry, :action),
        "actor_id" => optional_binary(entry, :actor_id),
        "session_id" => optional_binary(entry, :session_id),
        "outcome" => optional_binary(entry, :outcome),
        "service_identity" => optional_binary(entry, :service_identity),
        "details" => audit_details(entry)
      }

      {:ok, Jason.encode!(dto)}
    end
  rescue
    error -> {:error, {:auth_audit_encode_failed, error}}
  end

  def encode_audit(value), do: {:error, {:invalid_auth_audit, value}}

  @spec decode_audit(String.t()) :: {:ok, map()} | {:error, term()}
  def decode_audit(payload) when is_binary(payload) do
    with {:ok, %{"format" => @audit_format, "schema_version" => @schema_version} = dto} <-
           Jason.decode(payload),
         {:ok, id} <- required_binary(dto, "id"),
         {:ok, occurred_at} <- datetime_from_dto(Map.get(dto, "occurred_at")),
         {:ok, action} <- optional_binary_from_dto(dto, "action"),
         {:ok, actor_id} <- optional_binary_from_dto(dto, "actor_id"),
         {:ok, session_id} <- optional_binary_from_dto(dto, "session_id"),
         {:ok, outcome} <- optional_binary_from_dto(dto, "outcome"),
         {:ok, service_identity} <- optional_binary_from_dto(dto, "service_identity"),
         {:ok, details} <- details_from_dto(Map.get(dto, "details", %{})) do
      entry =
        details
        |> Map.merge(%{
          id: id,
          occurred_at: occurred_at,
          action: action,
          actor_id: actor_id,
          session_id: session_id,
          outcome: outcome,
          service_identity: service_identity
        })
        |> drop_nil_values()

      {:ok, entry}
    else
      {:ok, %{"format" => @audit_format, "schema_version" => version}} ->
        {:error, {:unsupported_auth_audit_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_auth_audit_dto, other}}

      {:error, %Jason.DecodeError{} = reason} ->
        {:error, {:invalid_auth_audit_json, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def decode_audit(value), do: {:error, {:invalid_auth_audit_payload, value}}

  defp roles_to_dto(roles) do
    Enum.reduce_while(roles, {:ok, []}, fn
      role, {:ok, acc} when role in @known_roles ->
        {:cont, {:ok, [Atom.to_string(role) | acc]}}

      role, _acc ->
        {:halt, {:error, {:unknown_auth_role, role}}}
    end)
    |> case do
      {:ok, values} -> {:ok, values |> Enum.reverse() |> Enum.uniq()}
      {:error, reason} -> {:error, reason}
    end
  end

  defp roles_from_dto(roles) when is_list(roles) do
    Enum.reduce_while(roles, {:ok, []}, fn role, {:ok, acc} ->
      case role_from_dto(role) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, values |> Enum.reverse() |> Enum.uniq()}
      {:error, reason} -> {:error, reason}
    end
  end

  defp roles_from_dto(value), do: {:error, {:invalid_auth_roles_field, :roles, value}}

  defp role_from_dto("viewer"), do: {:ok, :viewer}
  defp role_from_dto("operator"), do: {:ok, :operator}
  defp role_from_dto("admin"), do: {:ok, :admin}
  defp role_from_dto(role) when is_binary(role), do: {:error, {:unknown_auth_role, role}}
  defp role_from_dto(role), do: {:error, {:invalid_auth_role, role}}

  defp credential_from_dto(%{
         "kind" => "password_hash",
         "algorithm" => "argon2id",
         "password_hash" => "$argon2id$" <> _ = password_hash
       }) do
    {:ok, %{password_hash: password_hash}}
  end

  defp credential_from_dto(%{"kind" => "password_hash", "algorithm" => "argon2id"} = value),
    do: {:error, {:invalid_auth_credential_field, :credential, value}}

  defp credential_from_dto(%{"kind" => "password_hash", "algorithm" => algorithm})
       when is_binary(algorithm) do
    {:error, {:unsupported_auth_credential_algorithm, algorithm}}
  end

  defp credential_from_dto(value),
    do: {:error, {:invalid_auth_credential_field, :credential, value}}

  defp required_binary(entry, field) do
    case Map.get(entry, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:invalid_auth_audit_field, field, value}}
    end
  end

  defp optional_binary_from_dto(dto, field) do
    case Map.get(dto, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      nil -> {:ok, nil}
      value -> {:error, {:invalid_auth_audit_field, String.to_existing_atom(field), value}}
    end
  end

  defp required_datetime(entry, field) do
    case Map.get(entry, field) do
      %DateTime{} = value -> {:ok, value}
      value -> {:error, {:invalid_auth_audit_field, field, value}}
    end
  end

  defp optional_binary(entry, field) do
    case Map.get(entry, field) do
      value when is_binary(value) and value != "" -> value
      _value -> nil
    end
  end

  defp audit_details(entry) do
    entry
    |> Map.drop(@audit_fields)
    |> JsonSafe.data()
    |> drop_reserved_detail_keys()
  end

  defp datetime_from_dto(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _other -> {:error, {:invalid_auth_audit_field, :occurred_at, value}}
    end
  end

  defp datetime_from_dto(value), do: {:error, {:invalid_auth_audit_field, :occurred_at, value}}

  defp details_from_dto(details) when is_map(details),
    do: {:ok, drop_reserved_detail_keys(details)}

  defp details_from_dto(value), do: {:error, {:invalid_auth_audit_field, :details, value}}

  defp drop_reserved_detail_keys(details) when is_map(details) do
    Map.drop(details, @audit_field_strings)
  end

  defp drop_nil_values(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
