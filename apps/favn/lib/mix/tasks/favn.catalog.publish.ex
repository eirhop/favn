defmodule Mix.Tasks.Favn.Catalog.Publish do
  use Mix.Task
  @shortdoc "Publishes manifest and semantic SQL catalogs directly from CI"
  @moduledoc """
  Publishes trusted artifacts to an explicitly configured SQL target.

      mix favn.catalog.publish --manifest catalog.json --target analytics --expect-manifest none:0
      mix favn.catalog.publish --semantics semantic.json --target analytics --expect-semantics none:0

  Both inputs may be supplied together. `--reconcile` only reads the receipt for
  the same inputs and expectations. `--timeout-ms` defaults to 300000 (max 900000).
  `--config` defaults to `config/catalog_publish.exs`; it must not import the
  application's runtime config. See the SQL catalog publication guide.

  Use precompiled dependencies in an isolated CI project. This task does not
  compile, discover customer modules, run `app.config`/`app.start`, or boot a
  runner/orchestrator. Mix evaluates project build config before invoking tasks;
  that config and the dedicated publisher config are trusted deployment code.
  """
  @impl true
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          manifest: :string,
          semantics: :string,
          target: :string,
          expect_manifest: :string,
          expect_semantics: :string,
          config: :string,
          timeout_ms: :integer,
          reconcile: :boolean
        ]
      )

    result =
      if rest == [] and invalid == [] and
           length(Keyword.keys(opts)) == length(Enum.uniq(Keyword.keys(opts))) do
        run_config(opts)
      else
        {:error, %{"outcome" => "error", "reason" => "invalid_arguments"}}
      end

    case result do
      {:ok, receipt} ->
        Mix.shell().info(Jason.encode!(receipt))

      {:error, receipt} ->
        Mix.shell().info(Jason.encode!(receipt))
        Mix.raise("catalog publication did not complete; inspect the JSON result")
    end
  end

  defp run_config(opts) do
    path = Keyword.get(opts, :config, "config/catalog_publish.exs")
    config = Config.Reader.read!(path, env: Mix.env(), target: Mix.target())
    Favn.Catalog.publish(opts, Keyword.get(config, :favn, []))
  rescue
    _ -> {:error, %{"outcome" => "error", "reason" => "invalid_publisher_config"}}
  end
end
