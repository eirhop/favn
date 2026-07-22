defmodule FavnOrchestrator.RunSubmission.AssetOptionsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.RunSubmission.AssetOptions

  @asset %Asset{ref: {MyApp.Assets.Orders, :asset}, module: MyApp.Assets.Orders, name: :asset}

  test "normalizes string-keyed manifest request options" do
    assert {:ok, opts} =
             AssetOptions.from_input(@asset, %{
               "config" => %{
                 "dependencies" => "none",
                 "refresh" => "force_selected",
                 "metadata" => %{"source" => "operator"}
               }
             })

    assert opts[:dependencies] == :none
    assert opts[:refresh] == {:force_assets, [@asset.ref]}
    assert opts[:metadata] == %{"source" => "operator"}
  end

  test "rejects malformed metadata before selection enrichment" do
    assert {:error, :invalid_run_metadata} =
             AssetOptions.from_input(@asset, %{
               config: %{metadata: :invalid},
               selection: %{source: :refresh_timeline, id: "refresh:day:2026-05-12"}
             })
  end

  test "operator upstream refresh requires dependency traversal" do
    request = %AssetRunRequest{
      dependency_mode: :none,
      refresh_mode: :force_selected_upstream
    }

    assert {:error, {:refresh_include_upstream_requires_dependencies, :all}} =
             AssetOptions.from_operator_request(@asset, request)
  end

  test "translates every operator refresh mode at the runtime boundary" do
    cases = [
      {:auto, :all, :auto},
      {:missing, :all, :missing},
      {:force_all, :all, :force},
      {:force_selected, :none, {:force_assets, [@asset.ref]}},
      {:force_selected_upstream, :all, {:force_assets, [@asset.ref], [include_upstream: true]}}
    ]

    for {refresh_mode, dependency_mode, expected_refresh} <- cases do
      request = %AssetRunRequest{
        dependency_mode: dependency_mode,
        refresh_mode: refresh_mode
      }

      assert {:ok, opts} = AssetOptions.from_operator_request(@asset, request)
      assert opts[:dependencies] == dependency_mode
      assert opts[:refresh] == expected_refresh
    end
  end

  test "pins refresh selections and metadata to the displayed pipeline context" do
    context = run_context()

    request = %AssetRunRequest{
      run_context_id: context.id,
      selection: %{
        source: :refresh_timeline,
        id: "refresh:month:2026-07",
        kind: :month,
        value: "2026-07",
        timezone: "Europe/Oslo",
        run_id: nil
      }
    }

    assert {:ok, opts} = AssetOptions.from_operator_request(@asset, request, context)
    assert opts[:window_selection].intent == :manual
    assert opts[:window_selection].expansion == :none
    assert [anchor] = opts[:window_selection].effective_anchors
    assert anchor.kind == :month
    assert anchor.timezone == "Europe/Oslo"
    assert opts[:metadata].asset_run_context.id == context.id
    assert opts[:metadata].asset_run_context.policy.anchor == :current_period

    forged = put_in(request.selection.timezone, "Etc/UTC")

    assert {:error, {:asset_run_context_timezone_mismatch, "Europe/Oslo", "Etc/UTC"}} =
             AssetOptions.from_operator_request(@asset, forged, context)

    wrong_kind = %{request | selection: %{request.selection | id: "refresh:day:2026-07-17"}}

    assert {:error, {:asset_run_context_window_kind_mismatch, :month, :day}} =
             AssetOptions.from_operator_request(@asset, wrong_kind, context)
  end

  test "resolves the run context against the pinned manifest at the command boundary" do
    pipelines = [pipeline(:manual), pipeline(:scheduled)]
    version = version_fixture(pipelines, "multiple")

    assert {:error, :ambiguous_asset_run_context} =
             AssetOptions.from_operator_request(version, @asset, %AssetRunRequest{})

    scheduled_id = ManifestTarget.pipeline_id({__MODULE__.Scheduled, :scheduled})

    assert {:ok, selected_opts} =
             AssetOptions.from_operator_request(version, @asset, %AssetRunRequest{
               run_context_id: scheduled_id
             })

    assert selected_opts[:metadata].asset_run_context.id == scheduled_id

    assert {:error, :invalid_asset_run_context} =
             AssetOptions.from_operator_request(version, @asset, %AssetRunRequest{
               run_context_id: "pipeline:forged"
             })

    assert {:ok, automatic_opts} =
             AssetOptions.from_operator_request(
               version_fixture([pipeline(:scheduled)], "single"),
               @asset,
               %AssetRunRequest{}
             )

    assert automatic_opts[:metadata].asset_run_context.id == scheduled_id
  end

  defp run_context do
    policy = Policy.new!(:monthly, anchor: :current_period)
    pipeline = %Pipeline{module: MyApp.Pipelines.Monthly, name: :scheduled, window: policy}

    %AssetRunContext{
      id: "pipeline:scheduled",
      pipeline_ref: {pipeline.module, pipeline.name},
      pipeline: pipeline,
      index: %Index{},
      policy: policy,
      schedule_timezone: "Europe/Oslo",
      timezone: "Europe/Oslo"
    }
  end

  defp pipeline(:manual) do
    %Pipeline{
      module: __MODULE__.Manual,
      name: :manual,
      selectors: [{:asset, @asset.ref}],
      window: Policy.new!(:monthly, anchor: :previous_complete_period)
    }
  end

  defp pipeline(:scheduled) do
    %Pipeline{
      module: __MODULE__.Scheduled,
      name: :scheduled,
      selectors: [{:asset, @asset.ref}],
      window: Policy.new!(:monthly, anchor: :current_period)
    }
  end

  defp version_fixture(pipelines, suffix) do
    {:ok, graph} = Graph.build([@asset])

    %Version{
      manifest_version_id: "mv_asset_options_#{suffix}",
      content_hash: "sha256:asset-options-#{suffix}",
      manifest: %Manifest{assets: [@asset], pipelines: pipelines, graph: graph}
    }
  end
end
