defmodule Favn.Dev.ComposeSession do
  @moduledoc false

  alias Favn.Dev.{ComposeDeployment, State}

  @runtime_schema_version 6

  @type credentials :: %{service_token: String.t()}
  @type session_context :: %{
          required(String.t()) => String.t()
        }

  @spec resolve(keyword()) ::
          {:ok, String.t(), credentials(), session_context()} | {:error, term()}
  def resolve(opts) when is_list(opts) do
    with {:ok, runtime} <- read_running_runtime(opts),
         {:ok, deployment} <- ComposeDeployment.from_runtime(runtime, opts),
         {:ok, token} <- read_service_token(opts) do
      {:ok, deployment.orchestrator_url, %{service_token: token}, local_context(deployment)}
    end
  end

  defp read_running_runtime(opts) do
    case State.read_runtime(opts) do
      {:ok,
       %{
         "schema_version" => @runtime_schema_version,
         "kind" => "docker_compose",
         "compose_project" => project_name
       } = runtime}
      when is_binary(project_name) and project_name != "" ->
        {:ok, runtime}

      {:ok, _unsupported} ->
        {:error, :stack_not_running}

      {:error, :not_found} ->
        {:error, :stack_not_running}

      {:error, reason} ->
        {:error, {:local_runtime_state_unavailable, reason}}
    end
  end

  defp read_service_token(opts) do
    case State.read_secrets(opts) do
      {:ok, %{"service_token" => token}} when is_binary(token) and token != "" -> {:ok, token}
      _invalid -> {:error, :invalid_local_secrets}
    end
  end

  defp local_context(deployment) do
    %{
      "actor_id" => "local-dev-cli",
      "session_id" => "local-dev-cli",
      "local_dev_context" => "trusted",
      "workspace_id" => deployment.workspace_id
    }
  end
end
