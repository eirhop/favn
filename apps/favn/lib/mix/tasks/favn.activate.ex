defmodule Mix.Tasks.Favn.Activate do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Activates one staged manifest for one workspace"

  @moduledoc """
  Activates an exact manifest version after the control plane verifies the
  configured runner release. Authentication is accepted only through
  `FAVN_ORCHESTRATOR_SERVICE_TOKEN`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    case Dev.activate(opts) do
      {:ok, summary} ->
        IO.puts("Favn manifest activation complete")
        IO.puts("manifest version: #{summary.manifest_version_id}")
        IO.puts("workspace: #{summary.workspace_id}")
        IO.puts("activated: #{summary.activated?}")

      {:error, {:missing_required_env, name}} ->
        Mix.raise("activation failed: missing required environment variable #{name}")

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
        orchestrator_url: :string
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
