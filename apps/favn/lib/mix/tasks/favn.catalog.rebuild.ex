defmodule Mix.Tasks.Favn.Catalog.Rebuild do
  use Mix.Task
  @shortdoc "Rebuilds catalog SQL projections from retained artifacts"
  @moduledoc """
  Rebuilds derived metadata in the configured target without publishing new versions.

      mix favn.catalog.rebuild --target analytics

  Uses `--config` (default `config/catalog_publish.exs`) and `--timeout-ms`
  (default 300000, maximum 900000). Run in a precompiled, isolated CI project;
  no customer runtime, orchestrator or runner starts. Stop publishers first.
  Releases, selections, receipts, macros and business tables remain intact.
  An unknown commit is reported explicitly, never automatically retried.
  See the SQL catalog publication guide for maintenance limits and recovery.
  """
  @impl true
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [target: :string, config: :string, timeout_ms: :integer]
      )

    result =
      if rest == [] and invalid == [] and
           length(Keyword.keys(opts)) == length(Enum.uniq(Keyword.keys(opts))),
         do: run_config(opts),
         else: {:error, %{"outcome" => "error", "reason" => "invalid_arguments"}}

    case result do
      {:ok, result} ->
        Mix.shell().info(Jason.encode!(result))

      {:error, result} ->
        Mix.shell().info(Jason.encode!(result))
        Mix.raise("catalog rebuild did not complete; inspect the JSON result")
    end
  end

  defp run_config(opts) do
    config =
      Config.Reader.read!(Keyword.get(opts, :config, "config/catalog_publish.exs"),
        env: Mix.env(),
        target: Mix.target()
      )

    Favn.Catalog.rebuild(opts, Keyword.get(config, :favn, []))
  rescue
    _ -> {:error, %{"outcome" => "error", "reason" => "invalid_publisher_config"}}
  end
end
