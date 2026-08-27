defmodule Mix.Tasks.Favn.Activate do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Activates one staged manifest for one workspace"

  @moduledoc """
  Activates an exact manifest version after the control plane verifies the
  configured runner release. Authentication is accepted only through
  `FAVN_ORCHESTRATOR_SERVICE_TOKEN`.

  Running this explicit activation command also approves the manifest's
  validated execution-pool defaults for the target workspace. Existing
  workspace overrides remain in effect.

  `--timeout-ms` bounds the activation request (default: 360000; maximum:
  900000). If the response is lost or has an unknown gateway outcome, the
  command reconciles the exact active manifest for up to
  `--reconcile-timeout-ms` (default: 10000; maximum: 60000).
  An unproven outcome is reported as `activation_outcome_unknown` and is safe to
  retry only with the same `--operation-id`. Without that option, Favn creates
  a fresh operation id and reports it in both success and unknown outcomes.
  """

  alias Favn.CLI
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    case CLI.activate(opts) do
      {:ok, summary} ->
        IO.puts("Favn manifest activation complete")
        IO.puts("manifest version: #{summary.manifest_version_id}")
        IO.puts("workspace: #{summary.workspace_id}")
        IO.puts("activated: #{summary.activated?}")
        IO.puts("reconciled: #{summary.reconciled?}")
        IO.puts("operation id: #{summary.operation_id}")

      {:error, {:missing_required_env, name}} ->
        Mix.raise("activation failed: missing required environment variable #{name}")

      {:error, {:activation_outcome_unknown, details}} ->
        Mix.raise(
          "activation outcome unknown: manifest=#{details.manifest_version_id} " <>
            "workspace=#{details.workspace_id} operation_id=#{details.operation_id}; " <>
            details.retry_guidance
        )

      {:error, {:invalid_option, key}} ->
        Mix.raise("activation failed: invalid #{option_name(key)}")

      {:error, reason} ->
        Mix.raise("activation failed: #{inspect(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) do
    opts =
      CLIArgs.parse_no_args!("favn.activate", args,
        manifest_version: :string,
        workspace_id: :string,
        orchestrator_url: :string,
        timeout_ms: :integer,
        reconcile_timeout_ms: :integer,
        operation_id: :string
      )

    opts
    |> Keyword.put(:manifest_version_id, opts[:manifest_version])
    |> Keyword.delete(:manifest_version)
    |> put_url_default()
    |> require_options!([:manifest_version_id, :workspace_id, :orchestrator_url])
  end

  defp put_url_default(opts) do
    Keyword.put_new_lazy(opts, :orchestrator_url, fn ->
      System.get_env("FAVN_ORCHESTRATOR_URL")
    end)
  end

  defp require_options!(opts, keys) do
    missing = Enum.reject(keys, &(is_binary(opts[&1]) and opts[&1] != ""))

    if missing == [] do
      opts
    else
      names = Enum.map_join(missing, ", ", &option_name/1)
      Mix.raise("missing required option(s): #{names}")
    end
  end

  defp option_name(:manifest_version_id), do: "--manifest-version"
  defp option_name(key), do: "--" <> (key |> to_string() |> String.replace("_", "-"))
end
