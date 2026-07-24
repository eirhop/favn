defmodule Favn.DeploymentMode do
  @moduledoc """
  Parses the production control-plane deployment profile.

  Source development is composed by `favn_local` and does not use a production
  deployment-mode exception.
  """

  @environment_variable "FAVN_DEPLOYMENT_MODE"

  @type t :: :production

  @doc "Returns the deployment-mode environment variable name."
  @spec environment_variable() :: String.t()
  def environment_variable, do: @environment_variable

  @doc "Parses production by default."
  @spec from_env(map()) :: {:ok, t()} | {:error, term()}
  def from_env(env) when is_map(env) do
    case Map.get(env, @environment_variable) do
      nil -> {:ok, :production}
      "" -> {:ok, :production}
      "production" -> {:ok, :production}
      _invalid -> {:error, {:invalid_env, @environment_variable, "production"}}
    end
  end
end
