defmodule FavnOrchestrator.RunOwnership do
  @moduledoc """
  Workspace-scoped use cases for fenced, expiring run ownership.

  Ownership is PostgreSQL authority. A process registry or BEAM PID can improve
  routing, but neither may authorize a run transition.
  """

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @default_lease_duration_ms 30_000

  @doc "Returns the default ownership lease duration."
  @spec default_lease_duration_ms() :: pos_integer()
  def default_lease_duration_ms, do: @default_lease_duration_ms

  @doc "Builds a node-and-process-specific owner identity."
  @spec owner_id(String.t()) :: String.t()
  def owner_id(run_id) when is_binary(run_id) do
    instance = System.get_env("FAVN_INSTANCE_ID", Atom.to_string(node()))
    digest = :crypto.hash(:sha256, :erlang.term_to_binary({node(), self(), run_id}))
    "#{String.slice(instance, 0, 96)}:#{Base.url_encode64(digest, padding: false)}"
  end

  @doc "Claims one run or takes over an expired generation."
  @spec claim(WorkspaceContext.t(), String.t(), String.t(), keyword()) ::
          {:ok, Ownership.t()} | {:error, term()}
  def claim(%WorkspaceContext{} = context, run_id, owner_id, opts \\ [])
      when is_binary(run_id) and is_binary(owner_id) and is_list(opts) do
    lease_duration_ms = Keyword.get(opts, :lease_duration_ms, @default_lease_duration_ms)

    Persistence.stores().run_ownership.claim_run(%ClaimRun{
      workspace_context: context,
      command_id:
        Keyword.get_lazy(opts, :command_id, fn -> command_id("claim", run_id, owner_id) end),
      run_id: run_id,
      owner_id: owner_id,
      lease_duration_ms: lease_duration_ms
    })
  end

  @doc "Renews exactly one unexpired ownership generation."
  @spec renew(WorkspaceContext.t(), Ownership.t(), keyword()) ::
          {:ok, Ownership.t()} | {:error, term()}
  def renew(%WorkspaceContext{} = context, %Ownership{} = ownership, opts \\ []) do
    lease_duration_ms = Keyword.get(opts, :lease_duration_ms, @default_lease_duration_ms)

    renewal_id =
      Keyword.get_lazy(opts, :renewal_id, fn ->
        command_id("renew", ownership.run_id, ownership.owner_id)
      end)

    Persistence.stores().run_ownership.renew_run(%RenewRunOwnership{
      workspace_context: context,
      renewal_id: renewal_id,
      run_id: ownership.run_id,
      owner_id: ownership.owner_id,
      fencing_token: ownership.fencing_token,
      lease_duration_ms: lease_duration_ms
    })
  end

  @doc "Releases only the matching ownership generation."
  @spec release(WorkspaceContext.t(), Ownership.t()) :: :ok | {:error, term()}
  def release(%WorkspaceContext{} = context, %Ownership{} = ownership) do
    Persistence.stores().run_ownership.release_run(%ReleaseRunOwnership{
      workspace_context: context,
      run_id: ownership.run_id,
      owner_id: ownership.owner_id,
      fencing_token: ownership.fencing_token
    })
  end

  @doc "Claims one bounded recovery batch in a workspace."
  @spec claim_recovery_batch(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, [Ownership.t()]} | {:error, term()}
  def claim_recovery_batch(%WorkspaceContext{} = context, owner_id, opts \\ [])
      when is_binary(owner_id) and is_list(opts) do
    Persistence.stores().run_ownership.claim_recovery_batch(%ClaimRecoveryBatch{
      workspace_context: context,
      batch_id:
        Keyword.get_lazy(opts, :batch_id, fn ->
          command_id("recovery", context.workspace_id, owner_id)
        end),
      owner_id: owner_id,
      lease_duration_ms: Keyword.get(opts, :lease_duration_ms, @default_lease_duration_ms),
      limit: Keyword.get(opts, :limit, 100)
    })
  end

  defp command_id(operation, first, second) do
    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary({operation, first, second, :crypto.strong_rand_bytes(16)})
      )
      |> Base.url_encode64(padding: false)

    operation <> ":" <> digest
  end
end
