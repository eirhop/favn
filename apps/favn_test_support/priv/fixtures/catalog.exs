defmodule FavnTestSupport.CatalogFixture do
  alias Favn.Manifest.{
    Asset,
    ExecutionPackage,
    Graph,
    Pipeline,
    Publication,
    Schedule,
    SQLExecution,
    Version
  }

  alias Favn.SQL.{Contract, Template}
  alias Favn.Semantic.{Compiler, Snapshot}

  def source do
    %{
      ref: {Example.Sales, :asset},
      module: Example.Sales,
      type: :sql,
      depends_on: [],
      relation: %{connection: :warehouse, catalog: "mart", schema: "sales", name: "orders"},
      contract:
        Contract.new!(
          grain: [by: [:id]],
          columns: [
            %{name: :id, type: :integer, null: false, from: [{"sales_source.orders", "id"}]},
            %{name: :net, type: :decimal, null: false},
            %{name: :units, type: :integer, null: false}
          ]
        )
    }
  end

  def publication(description \\ "Sales") do
    source = source()
    sql = "SELECT 1 id, 90 net, 3 units"

    template =
      Template.compile!(sql,
        file: "/private/build/sales.sql",
        line: 1,
        module: Example.Sales,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} =
      ExecutionPackage.new(source.ref, %SQLExecution{
        sql: sql,
        template: template,
        contract: source.contract,
        checks: checks(source.contract)
      })

    asset = %Asset{
      ref: source.ref,
      module: source.module,
      name: :asset,
      type: :sql,
      relation: source.relation,
      description: description,
      execution_package_hash: package.content_hash,
      metadata: %{tags: ["sales"], category: "mart", private_path: "/private/build"}
    }

    schedule = %Schedule{
      module: Example.Schedules,
      name: :daily,
      ref: {Example.Schedules, :daily},
      cron: "0 0 * * *",
      timezone: "Etc/UTC",
      missed: :one,
      overlap: :queue_one
    }

    pipeline = %Pipeline{
      module: Example.Pipeline,
      name: :daily,
      selectors: [{:asset, source.ref}],
      schedule: {:ref, schedule.ref}
    }

    attrs =
      FavnTestSupport.with_manifest_contract(%{
        assets: [asset],
        pipelines: [pipeline],
        schedules: [schedule],
        graph: %Graph{nodes: [source.ref], topo_order: [source.ref]}
      })

    {:ok, version} = Version.new(struct!(Favn.Manifest, attrs))
    {:ok, publication} = Publication.from_parts(version, [package])
    publication
  end

  def full_manifest do
    publication = publication()
    manifest = publication.version.manifest
    [sales] = manifest.assets

    source = %Asset{
      ref: {Example.Source, :asset},
      module: Example.Source,
      name: :asset,
      type: :source,
      relation: %{connection: :warehouse, catalog: "mart", schema: "raw", name: "orders"}
    }

    elixir = %Asset{
      ref: {Example.Import, :asset},
      module: Example.Import,
      name: :asset,
      type: :elixir,
      depends_on: [source.ref],
      runtime_config: %{
        api: %{
          endpoint: Favn.RuntimeConfig.Ref.env!("EXAMPLE_API_HOST"),
          token: Favn.RuntimeConfig.Ref.secret_env!("EXAMPLE_API_SECRET")
        }
      }
    }

    {:ok, window} = Favn.Window.Spec.new(:day, timezone: "Etc/UTC")

    {:ok, coverage} =
      Favn.Coverage.Effective.resolve(
        Favn.Coverage.Spec.new!(from: ~D[2020-01-01], through: :latest_closed),
        window,
        nil
      )

    sales = %{
      sales
      | depends_on: [elixir.ref],
        window: window,
        coverage: coverage,
        retry_policy: Favn.Retry.Policy.new!(max_attempts: 3, backoff: 500),
        freshness: Favn.Freshness.Policy.from_value!({:daily, timezone: "Etc/UTC"}),
        materialization: :table,
        partition_spec: Favn.SQL.PartitionSpec.normalize!([:id])
    }

    [pipeline] = manifest.pipelines
    [schedule] = manifest.schedules

    inline = %{
      pipeline
      | name: :inline,
        schedule: {:inline, %{schedule | origin: :inline}},
        resource_recovery: Favn.ResourceRecovery.Policy.new!(:retry_remaining),
        window: Favn.Window.Policy.new!(:day, timezone: "Etc/UTC")
    }

    sales = FavnTestSupport.with_target_descriptor(%{sales | semantic_generation_id: nil})
    assets = [source, elixir, sales]
    {:ok, graph} = Graph.build(assets)

    manifest = %{
      manifest
      | assets: assets,
        graph: graph,
        pipelines: [pipeline, inline],
        environment: Favn.Manifest.Environment.new!(coverage_scope: [from: ~D[2020-01-01]]),
        execution_pools:
          Map.new(1..400, &{"pool_#{&1}", %Favn.ExecutionPool.Policy{max_concurrency: 2}}),
        connection_circuits: %{
          "warehouse" =>
            Favn.CircuitBreaker.Policy.new!(failure_threshold: 3, probe_after_ms: 1000)
        }
    }

    {:ok, version} = Version.new(FavnTestSupport.with_manifest_contract(manifest))

    {:ok, publication} =
      Publication.from_parts(version, Publication.packages_by_hash(publication) |> Map.values())

    {:ok, artifact} = Favn.Catalog.Artifact.new(publication)
    artifact
  end

  def manifest(description \\ "Sales") do
    {:ok, artifact} = Favn.Catalog.Artifact.new(publication(description))
    artifact
  end

  def semantic(description \\ "Average unit price") do
    model = %{
      name: :sales,
      module: Example.Sales,
      dimension: nil,
      hierarchies: [],
      time: nil,
      metrics: [
        %{
          name: :average_price,
          args: [:net, :units],
          sql: "SUM(@net) / NULLIF(SUM(@units), 0)",
          file: "/private/build/sales.ex",
          line: 1,
          opts: [unit: :ratio, time_aggregate: :aggregate, description: description]
        }
      ],
      file: "/private/build/sales.ex",
      line: 1
    }

    {:ok, artifact} =
      Compiler.compile([model], [source()], fn sql, inputs, _opts ->
        locations =
          Regex.scan(~r/\bSUM\s*\(/, sql, return: :index) |> Enum.map(fn [{at, _}] -> at end)

        {:ok,
         %{
           native_type: "DOUBLE",
           nullable: :unknown,
           runtime_version: "test",
           compiler_version: "fixture-v1",
           validation_profile: Map.new(inputs, &{&1.name, "DECIMAL(18,2)"}),
           aggregate_locations: locations
         }}
      end)

    artifact
  end

  defp checks(contract) do
    Enum.map(Contract.generated_check_specs(contract), fn spec ->
      template =
        Template.compile!(spec.sql,
          file: "contract.sql",
          line: 1,
          module: Example.Sales,
          scope: :query,
          enforce_query_root: true
        )

      Favn.SQL.Check.new!(
        Map.merge(spec, %{
          template: template,
          origin: :contract,
          uses_query?: true,
          uses_target?: false
        })
      )
    end)
  end

  def rehash(document),
    do:
      Map.put(
        document,
        "catalog_version",
        Snapshot.digest("mc_", Map.delete(document, "catalog_version"))
      )
end
