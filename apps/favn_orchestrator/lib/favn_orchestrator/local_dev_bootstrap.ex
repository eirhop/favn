defmodule FavnOrchestrator.LocalDevBootstrap do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.PlatformContext

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    with :ok <- provision_configured_workspaces(opts) do
      GenServer.start_link(__MODULE__, :ready, name: __MODULE__)
    end
  end

  @doc false
  @spec provision_configured_workspaces(keyword()) :: :ok | {:error, term()}
  def provision_configured_workspaces(opts \\ []) when is_list(opts) do
    provision_workspace =
      Keyword.get(opts, :provision_workspace, &ManifestStore.provision_workspace/1)

    clock = Keyword.get(opts, :clock, &DateTime.utc_now/0)

    with {:ok, workspace_ids} <- configured_workspace_ids(),
         {:ok, context} <-
           PlatformContext.new(
             "favn:local-dev-bootstrap",
             "local-dev-startup",
             [:platform_admin]
           ) do
      occurred_at = clock.()

      Enum.reduce_while(workspace_ids, :ok, fn workspace_id, :ok ->
        command = %ProvisionWorkspace{
          platform_context: context,
          workspace_id: workspace_id,
          slug: workspace_id,
          display_name: workspace_id,
          occurred_at: occurred_at
        }

        case provision_workspace.(command) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, {:workspace_provision_failed, workspace_id, reason}}}
        end
      end)
    end
  end

  @impl true
  def init(:ready), do: {:ok, :ready}

  defp configured_workspace_ids do
    case Application.get_env(:favn_orchestrator, :workspace_ids, []) do
      workspace_ids when is_list(workspace_ids) and workspace_ids != [] ->
        if Enum.all?(workspace_ids, &valid_workspace_id?/1) do
          {:ok, Enum.uniq(workspace_ids)}
        else
          {:error, :invalid_local_dev_workspace_ids}
        end

      _missing_or_invalid ->
        {:error, :local_dev_workspace_ids_required}
    end
  end

  defp valid_workspace_id?(workspace_id) do
    is_binary(workspace_id) and workspace_id != "" and byte_size(workspace_id) <= 255
  end
end
