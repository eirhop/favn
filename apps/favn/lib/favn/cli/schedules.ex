defmodule Favn.CLI.Schedules do
  @moduledoc """
  Operates workspace schedule activation through the running orchestrator.

  Schedule definitions come from the active manifest, but every workspace is
  disabled by default until an operator explicitly activates a reviewed
  definition.
  """

  alias Favn.CLI.Context
  alias Favn.CLI.OrchestratorClient

  @doc "Lists active-manifest schedules and their effective workspace state."
  def list(opts \\ []) do
    with {:ok, url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.list_schedules(url, credentials.service_token, context)
    end
  end

  @doc "Returns one stable schedule entry."
  def get(schedule_id, opts \\ []) do
    with {:ok, url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.get_schedule(url, credentials.service_token, context, schedule_id)
    end
  end

  @doc "Previews bounded future occurrences without submitting runs."
  def preview(schedule_id, opts \\ []) do
    with {:ok, url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.preview_schedule(
        url,
        credentials.service_token,
        context,
        schedule_id,
        Keyword.get(opts, :limit, 5)
      )
    end
  end

  @doc "Activates one reviewed definition for future occurrences."
  def activate(schedule_id, reason, opts \\ []),
    do: set_activation(schedule_id, reason, true, opts)

  @doc "Deactivates future occurrence submission without cancelling submitted runs."
  def deactivate(schedule_id, reason, opts \\ []),
    do: set_activation(schedule_id, reason, false, opts)

  defp set_activation(schedule_id, reason, enabled, opts) do
    with {:ok, url, credentials, context} <- Context.resolve(opts) do
      OrchestratorClient.set_schedule_activation(
        url,
        credentials.service_token,
        context,
        schedule_id,
        enabled,
        reason
      )
    end
  end
end
