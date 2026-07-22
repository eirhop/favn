defmodule Favn.DeploymentMode do
  @moduledoc """
  Parses the bounded control-plane deployment profile.

  Production is the default. `local-development` exists only for the generated
  Docker Compose topology and permits its loopback HTTP and private-bridge
  PostgreSQL connection contract.
  """

  @environment_variable "FAVN_DEPLOYMENT_MODE"

  @type t :: :production | :local_development

  @doc "Returns the deployment-mode environment variable name."
  @spec environment_variable() :: String.t()
  def environment_variable, do: @environment_variable

  @doc "Parses production by default or the explicit local-development profile."
  @spec from_env(map()) :: {:ok, t()} | {:error, term()}
  def from_env(env) when is_map(env) do
    case Map.get(env, @environment_variable) do
      nil -> {:ok, :production}
      "" -> {:ok, :production}
      "production" -> {:ok, :production}
      "local-development" -> {:ok, :local_development}
      _invalid -> {:error, {:invalid_env, @environment_variable, "production|local-development"}}
    end
  end
end
