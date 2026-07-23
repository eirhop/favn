defmodule FavnOrchestrator.RunnerReplacement do
  @moduledoc """
  Owns the resumable admission boundary used while replacing one runner.

  Beginning replacement blocks new control-plane mutations while operations
  already admitted before the boundary are allowed to finish. The opaque lease
  token admits only the deployment owner until replacement completes or is
  aborted. This boundary is separate from the monotonic shutdown drain.
  """

  alias Favn.RunnerRelease
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunnerDiagnostics
  alias FavnOrchestrator.RuntimeConfig

  @doc "Begins or resumes runner replacement with its opaque admission token."
  @spec begin(String.t()) :: {:ok, String.t()} | {:error, term()}
  def begin(token) when is_binary(token),
    do: Lifecycle.begin_maintenance(:runner_replacement, token)

  @doc "Ends runner replacement when the opaque admission token matches."
  @spec finish(String.t()) :: :ok | {:error, term()}
  def finish(token) when is_binary(token) and token != "",
    do: Lifecycle.end_maintenance(token)

  @doc "Returns bounded runner-replacement drain state without its token."
  @spec status() :: map()
  def status do
    Lifecycle.diagnostics()
    |> Map.take([
      :status,
      :ready?,
      :accepting?,
      :active_admissions,
      :maintenance?,
      :maintenance_kind,
      :maintenance_started_at
    ])
  end

  @doc "Probes the remote runner and requires one exact logical release ID."
  @spec verify_runner(String.t()) :: {:ok, map()} | {:error, term()}
  def verify_runner(expected_release_id) when is_binary(expected_release_id) do
    runtime = RuntimeConfig.current()
    client = runtime.runner_client
    opts = runtime.runner_client_opts

    with :ok <- RunnerRelease.validate_id(expected_release_id),
         :ok <- RunnerClientValidator.validate(client),
         true <- function_exported?(client, :diagnostics, 1),
         {:ok, diagnostics} when is_map(diagnostics) <- client.diagnostics(opts),
         {:ok, actual_release_id} <- RunnerDiagnostics.validate_ready(diagnostics, opts),
         true <- actual_release_id == expected_release_id do
      {:ok,
       %{
         runner_release_id: actual_release_id,
         ready?: true,
         identity_source: field(diagnostics, :identity_source),
         node_name: field(diagnostics, :node_name)
       }}
    else
      false -> {:error, :runner_release_mismatch}
      {:error, _reason} = error -> error
      _invalid -> {:error, :runner_diagnostics_unavailable}
    end
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
