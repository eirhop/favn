Code.require_file("support.exs", __DIR__)
alias RegistrationStress.Support, as: S
{opts, []} = S.options(System.argv(), [revision: :string], revision: "HEAD")

if opts[:help] do
  IO.puts("elixir prepare.exs [--revision HEAD]")
else
  revision = S.revision(opts.revision)
  state = Path.join(S.root(), ".favn/registration-stress")

  harness_hash =
    ["prepare.exs", "support.exs", "config.exs", "runtime.exs", "operator.sh"]
    |> Enum.map(&File.read!(Path.join(__DIR__, &1)))
    |> IO.iodata_to_binary()
    |> S.hash()

  context =
    Path.join(
      state,
      "source-#{String.slice(revision, 0, 12)}-#{String.slice(harness_hash, 0, 8)}"
    )

  S.archive(revision, context)
  project = Path.join(context, "examples/basic-workflow-tutorial")
  deploy = Path.join(project, "deploy/favn")
  File.mkdir_p!(deploy)
  templates = Path.join(context, "apps/favn/priv/templates/deployment")

  for name <- ["mix.exs", "env.sh.eex"],
      do: File.cp!(Path.join(templates, name), Path.join(deploy, name))

  File.write!(
    Path.join(project, "config/config.exs"),
    "import Config\nimport_config \"registration_stress.exs\"\n"
  )

  File.cp!(Path.join(__DIR__, "config.exs"), Path.join(project, "config/registration_stress.exs"))
  File.cp!(Path.join(__DIR__, "runtime.exs"), Path.join(project, "config/runtime.exs"))
  sql = Path.join(project, "priv/duckdb")

  File.write!(
    Path.join(sql, "stress_startup.sql"),
    "SET threads=1; SET memory_limit='384MB'; LOAD postgres; LOAD ducklake; LOAD json;\n"
  )

  for catalog <- ["source", "core", "mart"] do
    File.write!(
      Path.join(sql, "#{catalog}_catalog.sql"),
      "ATTACH @metadata AS #{catalog} (METADATA_SCHEMA 'stress_#{catalog}', DATA_PATH '/var/lib/favn/stress/#{catalog}');\n"
    )
  end

  targets = for n <- 1..35, do: {n, n |> Integer.to_string() |> String.pad_leading(2, "0")}

  modules =
    for {n, padded} <- targets do
      """
      defmodule CrmDemo.RegistrationStress.Target#{padded} do
        @moduledoc "Local initial-registration stress target #{n}."
        use Favn.SQLAsset
        relation(connection: :warehouse, catalog: "source", schema: "stress", name: "target_#{padded}")
        materialized(:table)
        execution_pool(:duckdb)
        contract do
          column(:id, :integer, null: false)
        end
        query do
          ~SQL"SELECT i::BIGINT AS id FROM range(1000) AS t(i)"
        end
      end
      """
    end

  assets =
    Enum.map_join(targets, ", ", fn {_, padded} ->
      "{CrmDemo.RegistrationStress.Target#{padded}, :asset}"
    end)

  pipeline = """
  defmodule CrmDemo.RegistrationStress.Pipeline do
    @moduledoc "Local 35-target registration reproduction."
    use Favn.Pipeline
    pipeline :registration_stress do
      assets([#{assets}])
      max_concurrency(5)
      execution_pool(:duckdb)
    end
  end
  """

  source = Enum.join(modules ++ [pipeline], "\n")
  File.write!(Path.join(project, "lib/registration_stress.ex"), source)

  dockerfile =
    File.read!(Path.join(templates, "runner.Dockerfile"))
    |> String.replace("ENV MIX_ENV=prod", ~s(ENV MIX_ENV=prod ERL_FLAGS="+JMsingle true"))

  operator = """

  FROM builder AS local-operator
  COPY . /build
  WORKDIR /build/examples/basic-workflow-tutorial
  ARG FAVN_RUNNER_RELEASE_ID
  ENV DUCKDB_ADBC_DRIVER=/opt/duckdb/1.5.5/libduckdb.so
  RUN mix deps.get --only prod --check-locked && mix deps.compile && mix compile --warnings-as-errors && mix favn.build.manifest --runner-release "default=$FAVN_RUNNER_RELEASE_ID"
  COPY LocalOperator.sh /usr/local/bin/favn-simulation-operator
  RUN chmod 0555 /usr/local/bin/favn-simulation-operator
  ENTRYPOINT ["/usr/local/bin/favn-simulation-operator"]
  CMD ["help"]
  """

  File.write!(Path.join(context, "RunnerDockerfile"), dockerfile <> operator)
  File.cp!(Path.join(__DIR__, "operator.sh"), Path.join(context, "LocalOperator.sh"))
  profile_hash = S.hash(harness_hash <> source)
  tag = String.slice(revision, 0, 12) <> "-" <> String.slice(profile_hash, 0, 8)
  release = "rr_" <> S.hash(revision <> profile_hash)

  metadata = %{
    source_revision: revision,
    profile_sha256: profile_hash,
    runner_release_id: release,
    image_tag: tag,
    context: context
  }

  S.json_file(Path.join(state, "build.json"), metadata)

  File.write!(Path.join(state, "build.env"), """
  FAVN_SOURCE_REVISION=#{revision}
  FAVN_STRESS_CONTEXT=#{context}
  FAVN_STRESS_TAG=#{tag}
  FAVN_RUNNER_RELEASE_ID=#{release}
  FAVN_STRESS_CONTROL_IMAGE=ghcr.io/eirhop/favn-control-plane@sha256:641d01af54cc11264b5a16e459d460ba48f2ac9b80709b81e935a4e7399a2beb
  """)

  S.emit(metadata)
end
