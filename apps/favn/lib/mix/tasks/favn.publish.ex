defmodule Mix.Tasks.Favn.Publish do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Publishes an immutable manifest release as staged"

  @moduledoc """
  Publishes missing execution packages followed by one manifest index.
  Authentication is accepted only through `FAVN_ORCHESTRATOR_SERVICE_TOKEN`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    case Dev.publish(opts) do
      {:ok, summary} ->
        IO.puts("Favn manifest publication complete")
        IO.puts("manifest version: #{summary.manifest_version_id}")
        IO.puts("runner release: #{summary.required_runner_release_id}")
        IO.puts("status: #{summary.status}")

      {:error, {:missing_required_env, name}} ->
        Mix.raise("publication failed: missing required environment variable #{name}")

      {:error, reason} ->
        Mix.raise("publication failed: #{inspect(reason)}")
    end
  end

  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) do
    opts =
      CLIArgs.parse_no_args!("favn.publish", args, manifest: :string, orchestrator_url: :string)

    opts
    |> Keyword.put(:manifest_path, opts[:manifest])
    |> Keyword.delete(:manifest)
    |> put_url_default()
    |> require_options!([:manifest_path, :orchestrator_url])
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
      names =
        Enum.map_join(
          missing,
          ", ",
          &"--#{&1 |> to_string() |> String.replace("_path", "") |> String.replace("_", "-")}"
        )

      Mix.raise("missing required option(s): #{names}")
    end
  end
end
