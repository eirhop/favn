defmodule FavnOrchestrator.ManifestDeploymentContext do
  @moduledoc """
  Narrow authority for one authenticated manifest deployment workspace.

  This context is accepted only by the manifest deployment facade. It cannot
  be converted into general workspace or platform operator authority.
  """

  @enforce_keys [:service_identity, :workspace_id, :request_id]
  defstruct [:service_identity, :workspace_id, :request_id]

  @type t :: %__MODULE__{
          service_identity: String.t(),
          workspace_id: String.t(),
          request_id: String.t()
        }

  @doc "Builds a validated deployment-only request context."
  @spec new(String.t(), String.t(), String.t()) :: {:ok, t()} | {:error, :invalid_context}
  def new(service_identity, workspace_id, request_id)
      when is_binary(service_identity) and is_binary(workspace_id) and is_binary(request_id) do
    if valid_id?(service_identity, 128) and valid_id?(workspace_id, 255) and
         valid_id?(request_id, 255) do
      {:ok,
       %__MODULE__{
         service_identity: service_identity,
         workspace_id: workspace_id,
         request_id: request_id
       }}
    else
      {:error, :invalid_context}
    end
  end

  def new(_service_identity, _workspace_id, _request_id), do: {:error, :invalid_context}

  @doc "Returns whether a deployment request context is structurally valid."
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = context) do
    valid_id?(context.service_identity, 128) and valid_id?(context.workspace_id, 255) and
      valid_id?(context.request_id, 255)
  end

  def valid?(_context), do: false

  defp valid_id?(value, max),
    do: value != "" and byte_size(value) <= max and String.valid?(value)
end
