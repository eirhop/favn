defmodule FavnOrchestrator.WorkspaceConfiguration do
  @moduledoc """
  Non-secret runtime configuration frozen with one workspace deployment.

  The DTO is safe to return to operator clients. It identifies both the active
  deployment and the manifest-authored source of each value so clients never
  need to read the customer application's local configuration.
  """

  alias Favn.Manifest
  alias Favn.Manifest.Environment
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @configuration_key "workspace_environment"

  @enforce_keys [
    :workspace_id,
    :default_timezone,
    :default_timezone_source
  ]
  defstruct @enforce_keys ++ [:deployment_id]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          deployment_id: String.t() | nil,
          default_timezone: String.t(),
          default_timezone_source: Environment.timezone_source() | :no_active_deployment
        }

  @doc "Stores one manifest environment in deployment configuration."
  @spec put(map(), Manifest.t()) :: {:ok, map()} | {:error, term()}
  def put(configuration, %Manifest{environment: environment}) when is_map(configuration) do
    with {:ok, environment} <- Environment.from_manifest(environment) do
      {:ok,
       configuration
       |> Map.delete(:schema_version)
       |> Map.delete(:workspace_environment)
       |> Map.put("schema_version", 2)
       |> Map.put(@configuration_key, %{
         "schema_version" => 1,
         "environment" => Environment.to_map(environment)
       })}
    end
  end

  @doc "Returns the explicit UTC state used before a workspace has an active deployment."
  @spec without_active_deployment(String.t()) :: t()
  def without_active_deployment(workspace_id) when is_binary(workspace_id) do
    %__MODULE__{
      workspace_id: workspace_id,
      deployment_id: nil,
      default_timezone: "Etc/UTC",
      default_timezone_source: :no_active_deployment
    }
  end

  @doc "Returns the active workspace configuration through workspace authority."
  @spec active(WorkspaceContext.t()) :: {:ok, t()} | {:error, term()}
  def active(%WorkspaceContext{} = context) do
    case ManifestStore.get_active_deployment_configuration(context) do
      {:ok, {deployment_id, configuration}} ->
        with {:ok, environment} <- from_configuration(configuration) do
          {:ok,
           %__MODULE__{
             workspace_id: context.workspace_id,
             deployment_id: deployment_id,
             default_timezone: environment.default_timezone,
             default_timezone_source: environment.default_timezone_source
           }}
        end

      {:error, %FavnOrchestrator.Persistence.Error{kind: :not_found}} ->
        {:ok, without_active_deployment(context.workspace_id)}

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Validates and returns the workspace environment in persisted configuration."
  @spec from_configuration(map()) :: {:ok, Environment.t()} | {:error, term()}
  def from_configuration(configuration) when is_map(configuration) do
    case field(configuration, @configuration_key) do
      %{} = value ->
        with 1 <- field(value, "schema_version"),
             %{} = environment <- field(value, "environment"),
             {:ok, environment} <- Environment.from_manifest(environment) do
          {:ok, environment}
        else
          version when is_integer(version) ->
            {:error, {:unsupported_workspace_environment_version, version}}

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :invalid_workspace_environment}
        end

      _invalid ->
        {:error, :workspace_environment_required}
    end
  end

  def from_configuration(_configuration), do: {:error, :invalid_workspace_environment}

  @doc "Returns the JSON key used inside immutable deployment configuration."
  @spec configuration_key() :: String.t()
  def configuration_key, do: @configuration_key

  defp field(map, key) when is_binary(key) do
    atom_key = safe_existing_atom(key)

    cond do
      Map.has_key?(map, key) -> Map.get(map, key)
      not is_nil(atom_key) and Map.has_key?(map, atom_key) -> Map.get(map, atom_key)
      true -> nil
    end
  end

  defp safe_existing_atom(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end
end
