defmodule Favn.Dev.Rebuild do
  @moduledoc """
  Local operator workflow for planning and controlling asset rebuilds.

  Rebuild plans are immutable and require a separate, explicit start command.
  Every operation goes through the local orchestrator HTTP API; this module does
  not access the control-plane database directly.
  """

  alias Favn.Dev.ComposeSession
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Run

  @type workflow_opts :: [root_dir: Path.t()]

  @doc "Plans a rebuild for one asset in the active manifest."
  @spec plan(module() | String.t(), String.t(), workflow_opts()) ::
          {:ok, map()} | {:error, term()}
  def plan(asset, reason, opts \\ [])

  def plan(asset, reason, opts)
      when (is_atom(asset) or is_binary(asset)) and is_binary(reason) and reason != "" and
             is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts),
         {:ok, target_id} <- resolve_asset(base_url, credentials.service_token, context, asset) do
      OrchestratorClient.plan_rebuild(
        base_url,
        credentials.service_token,
        context,
        target_id,
        reason
      )
    end
  end

  def plan(_asset, _reason, _opts), do: {:error, :invalid_rebuild_plan}

  @doc "Starts an explicitly approved immutable rebuild plan."
  @spec start(String.t(), String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def start(plan_id, plan_hash, opts \\ [])
      when is_binary(plan_id) and is_binary(plan_hash) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts) do
      OrchestratorClient.start_rebuild(
        base_url,
        credentials.service_token,
        context,
        plan_id,
        plan_hash
      )
    end
  end

  @doc "Fetches one rebuild operation."
  @spec status(String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def status(operation_id, opts \\ []) when is_binary(operation_id) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts) do
      OrchestratorClient.get_rebuild(
        base_url,
        credentials.service_token,
        context,
        operation_id
      )
    end
  end

  @doc "Requests cancellation of one rebuild operation."
  @spec cancel(String.t(), String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def cancel(operation_id, reason, opts \\ [])
      when is_binary(operation_id) and is_binary(reason) and reason != "" and is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts) do
      OrchestratorClient.cancel_rebuild(
        base_url,
        credentials.service_token,
        context,
        operation_id,
        reason
      )
    end
  end

  @doc "Retries a failed rebuild with its original immutable plan hash."
  @spec retry(String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def retry(operation_id, opts \\ []) when is_binary(operation_id) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts),
         {:ok, rebuild} <-
           OrchestratorClient.get_rebuild(
             base_url,
             credentials.service_token,
             context,
             operation_id
           ),
         {:ok, plan_hash} <- fetch_plan_hash(rebuild) do
      OrchestratorClient.retry_rebuild(
        base_url,
        credentials.service_token,
        context,
        operation_id,
        plan_hash
      )
    end
  end

  @doc "Requests explicit reconciliation of an unknown rebuild outcome."
  @spec reconcile(String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def reconcile(operation_id, opts \\ []) when is_binary(operation_id) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- ComposeSession.resolve(opts) do
      OrchestratorClient.reconcile_rebuild(
        base_url,
        credentials.service_token,
        context,
        operation_id
      )
    end
  end

  defp resolve_asset(base_url, service_token, context, asset) do
    with {:ok, manifest} <- OrchestratorClient.active_manifest(base_url, service_token, context),
         {:ok, %{"target_type" => "asset", "target_id" => target_id}} <-
           Run.resolve_run_target(manifest, asset) do
      {:ok, target_id}
    else
      {:ok, %{"target_type" => _other}} -> {:error, :rebuild_requires_asset}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_asset_target}
    end
  end

  defp fetch_plan_hash(%{"plan_hash" => hash}) when is_binary(hash) and hash != "", do: {:ok, hash}
  defp fetch_plan_hash(_rebuild), do: {:error, :invalid_rebuild_response}
end
