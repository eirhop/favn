defmodule FavnView.Auth.AzureContainerAppsEntra do
  @moduledoc """
  Parses the signed-in principal injected by Azure Container Apps Easy Auth.

  The Container Apps authentication sidecar validates Microsoft Entra tokens
  before the request reaches Favn and replaces the `x-ms-client-principal`
  header. This boundary accepts only the immutable tenant (`tid`) and object
  (`oid`) claims. It never reads access-token headers, email, display name,
  groups, or provider roles.

  This adapter is safe only when callers cannot bypass Container Apps ingress
  and reach the Favn HTTP listener directly.
  """

  import Plug.Conn, only: [get_req_header: 2]

  @provider "azure_container_apps_entra"
  @max_header_bytes 16_384
  @max_claims 128
  @tenant_claims [
    "tid",
    "http://schemas.microsoft.com/identity/claims/tenantid"
  ]
  @subject_claims [
    "oid",
    "http://schemas.microsoft.com/identity/claims/objectidentifier"
  ]
  @uuid ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

  @type identity :: %{
          provider: String.t(),
          tenant_id: String.t(),
          subject_id: String.t()
        }

  @doc "Parses one Easy Auth principal and enforces the configured tenant."
  @spec identity(Plug.Conn.t(), String.t()) :: {:ok, identity()} | {:error, :invalid_principal}
  def identity(conn, expected_tenant_id) when is_binary(expected_tenant_id) do
    case get_req_header(conn, "x-ms-client-principal") do
      [encoded] -> from_header_value(encoded, expected_tenant_id)
      _missing_or_ambiguous -> {:error, :invalid_principal}
    end
  end

  @doc false
  @spec from_header_value(String.t(), String.t()) ::
          {:ok, identity()} | {:error, :invalid_principal}
  def from_header_value(encoded, expected_tenant_id)
      when is_binary(encoded) and is_binary(expected_tenant_id) and
             byte_size(encoded) <= @max_header_bytes do
    with {:ok, decoded} <- Base.decode64(encoded),
         {:ok, %{"auth_typ" => auth_type, "claims" => claims}} <- Jason.decode(decoded),
         true <- String.downcase(auth_type) == "aad",
         true <- is_list(claims) and length(claims) <= @max_claims,
         :ok <- validate_claim_shapes(claims),
         {:ok, tenant_id} <- unique_claim(claims, @tenant_claims),
         {:ok, subject_id} <- unique_claim(claims, @subject_claims),
         {:ok, tenant_id} <- normalize_uuid(tenant_id),
         {:ok, subject_id} <- normalize_uuid(subject_id),
         {:ok, expected_tenant_id} <- normalize_uuid(expected_tenant_id),
         true <- Plug.Crypto.secure_compare(tenant_id, expected_tenant_id) do
      {:ok,
       %{
         provider: @provider,
         tenant_id: tenant_id,
         subject_id: subject_id
       }}
    else
      _invalid -> {:error, :invalid_principal}
    end
  rescue
    _invalid -> {:error, :invalid_principal}
  end

  def from_header_value(_encoded, _expected_tenant_id), do: {:error, :invalid_principal}

  defp validate_claim_shapes(claims) do
    if Enum.all?(claims, fn
         %{"typ" => type, "val" => value}
         when is_binary(type) and byte_size(type) in 1..512 and is_binary(value) and
                byte_size(value) in 1..2_048 ->
           true

         _invalid ->
           false
       end),
       do: :ok,
       else: {:error, :invalid_principal}
  end

  defp unique_claim(claims, accepted_types) do
    values =
      claims
      |> Enum.filter(&(Map.fetch!(&1, "typ") in accepted_types))
      |> Enum.map(&Map.fetch!(&1, "val"))
      |> Enum.uniq()

    case values do
      [value] -> {:ok, value}
      _missing_or_conflicting -> {:error, :invalid_principal}
    end
  end

  defp normalize_uuid(value) do
    normalized = value |> String.trim() |> String.downcase()
    if Regex.match?(@uuid, normalized), do: {:ok, normalized}, else: {:error, :invalid_principal}
  end
end
