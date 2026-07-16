defmodule Favn.Azure.Credentials.Request do
  @moduledoc """
  Normalized request identifying one Azure access token cache entry.

  The request contains identity selection, never a resolved credential.
  """

  alias Favn.Azure.TokenError

  @max_resource_bytes 4_096
  @max_client_id_bytes 1_024

  @enforce_keys [:resource, :provider]
  defstruct [:resource, :provider, :client_id, endpoint: :auto]

  @type provider :: :azure_cli | :managed_identity | module()
  @type endpoint :: :auto | :imds | :azure_app_service
  @type t :: %__MODULE__{
          resource: String.t(),
          provider: provider(),
          client_id: String.t() | nil,
          endpoint: endpoint()
        }

  @doc "Builds and validates an Azure credential request."
  @spec new(String.t(), keyword() | map()) :: {:ok, t()} | {:error, TokenError.t()}
  def new(resource, opts \\ [])

  def new(resource, opts) when is_map(opts), do: new(resource, Map.to_list(opts))

  def new(resource, opts) when is_binary(resource) and is_list(opts) do
    with true <-
           resource != "" and byte_size(resource) <= @max_resource_bytes and
             String.valid?(resource),
         true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- [:provider, :client_id, :endpoint],
         {:ok, provider} <- normalize_provider(Keyword.get(opts, :provider)),
         {:ok, client_id} <- normalize_client_id(Keyword.get(opts, :client_id)),
         {:ok, endpoint} <- normalize_endpoint(Keyword.get(opts, :endpoint, :auto)) do
      {:ok,
       %__MODULE__{
         resource: resource,
         provider: provider,
         client_id: client_id,
         endpoint: endpoint
       }}
    else
      _other -> invalid_request()
    end
  end

  def new(_resource, _opts), do: invalid_request()

  @doc "Builds an Azure credential request and raises for invalid deployment configuration."
  @spec new!(String.t(), keyword() | map()) :: t()
  def new!(resource, opts \\ []) do
    case new(resource, opts) do
      {:ok, request} -> request
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  defp normalize_provider(provider)
       when provider in [:azure_cli, :managed_identity],
       do: {:ok, provider}

  defp normalize_provider(provider) when is_atom(provider) and not is_nil(provider),
    do: {:ok, provider}

  defp normalize_provider(_provider), do: :error

  defp normalize_client_id(nil), do: {:ok, nil}
  defp normalize_client_id(value)
       when is_binary(value) and value != "" and byte_size(value) <= @max_client_id_bytes do
    if String.valid?(value), do: {:ok, value}, else: :error
  end
  defp normalize_client_id(_value), do: :error

  defp normalize_endpoint(value) when value in [:auto, :imds, :azure_app_service],
    do: {:ok, value}

  defp normalize_endpoint(_value), do: :error

  defp invalid_request do
    {:error,
     %TokenError{
       type: :invalid_config,
       message: "invalid Azure credential request",
       details: %{reason: :invalid_request}
     }}
  end
end

defimpl Inspect, for: Favn.Azure.Credentials.Request do
  import Inspect.Algebra

  def inspect(request, opts) do
    concat([
      "#Favn.Azure.Credentials.Request<",
      to_doc(
        [
          resource: request.resource,
          provider: request.provider,
          client_id: request.client_id,
          endpoint: request.endpoint
        ],
        opts
      ),
      ">"
    ])
  end
end
