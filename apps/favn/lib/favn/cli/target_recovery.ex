defmodule Favn.CLI.TargetRecovery do
  @moduledoc """
  Local operator workflow for evidence-backed recovery of an interrupted target.

  Plans are immutable and must be started explicitly. Every operation goes
  through the orchestrator API; this module never accesses the database directly.
  """

  alias Favn.CLI.Context
  alias Favn.CLI.OrchestratorClient
  alias Favn.CLI.Run

  @type workflow_opts :: [root_dir: Path.t()]

  @doc "Plans recovery for one asset in the active manifest."
  @spec plan(module() | String.t(), String.t(), workflow_opts()) ::
          {:ok, map()} | {:error, term()}
  def plan(asset, reason, opts \\ [])

  def plan(asset, reason, opts)
      when (is_atom(asset) or is_binary(asset)) and is_binary(reason) and reason != "" and
             is_list(opts) do
    with {:ok, base_url, credentials, context} <- Context.resolve(opts),
         {:ok, target_id} <- resolve_asset(base_url, credentials.service_token, context, asset) do
      OrchestratorClient.plan_target_recovery(
        base_url,
        credentials.service_token,
        context,
        target_id,
        reason
      )
    end
  end

  def plan(_asset, _reason, _opts), do: {:error, :invalid_target_recovery_plan}

  @doc "Starts an explicitly approved target-recovery plan."
  @spec start(String.t(), String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def start(plan_id, plan_hash, opts \\ [])
      when is_binary(plan_id) and is_binary(plan_hash) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.start_target_recovery(
        base_url,
        credentials.service_token,
        context,
        plan_id,
        plan_hash
      )
    end
  end

  @doc "Fetches one target-recovery operation."
  @spec status(String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def status(operation_id, opts \\ []) when is_binary(operation_id) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.get_target_recovery(
        base_url,
        credentials.service_token,
        context,
        operation_id
      )
    end
  end

  @doc "Reconciles an inconclusive marker outcome without repeating the marker write."
  @spec reconcile(String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def reconcile(operation_id, opts \\ []) when is_binary(operation_id) and is_list(opts) do
    with {:ok, base_url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.reconcile_target_recovery(
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
      {:ok, %{"target_type" => _other}} -> {:error, :target_recovery_requires_asset}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_asset_target}
    end
  end
end
