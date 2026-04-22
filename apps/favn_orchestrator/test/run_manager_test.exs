defmodule FavnOrchestrator.RunManagerTest do
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(execution_id, _timeout, _opts) do
      {:ok,
       %RunnerResult{
         status: :ok,
         asset_results: [asset_result(execution_id, :ok)],
         metadata: %{stub: true}
       }}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    defp asset_result(execution_id, status) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: nil,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      case work.asset_ref do
        {module, name} when is_atom(module) and is_atom(name) ->
          "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"

        _other ->
          "exec_#{work.run_id}"
      end
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientFlakyStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(execution_id, _timeout, opts) do
      counter = Keyword.fetch!(opts, :attempt_counter)

      attempt = Agent.get_and_update(counter, fn value -> {value + 1, value + 1} end)

      if attempt == 1 do
        {:ok,
         %RunnerResult{
           status: :error,
           error: :transient_failure,
           asset_results: [asset_result(execution_id, :error, :transient_failure)],
           metadata: %{attempt: attempt}
         }}
      else
        {:ok,
         %RunnerResult{
           status: :ok,
           asset_results: [asset_result(execution_id, :ok, nil)],
           metadata: %{attempt: attempt}
         }}
      end
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    defp asset_result(execution_id, status, error) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: error,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      case work.asset_ref do
        {module, name} when is_atom(module) and is_atom(name) ->
          "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"

        _other ->
          "exec_#{work.run_id}"
      end
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientSlowCancelableStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(execution_id, _timeout, opts) do
      block_ms = Keyword.get(opts, :block_ms, 200)
      Process.sleep(block_ms)

      {:ok,
       %RunnerResult{
         status: :ok,
         asset_results: [asset_result(execution_id, :ok)],
         metadata: %{stub: :slow}
       }}
    end

    @impl true
    def cancel_work(execution_id, reason, opts) do
      cancel_log = Keyword.fetch!(opts, :cancel_log)
      Agent.update(cancel_log, fn values -> [{execution_id, reason} | values] end)
      :ok
    end

    defp asset_result(execution_id, status) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: nil,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      case work.asset_ref do
        {module, name} when is_atom(module) and is_atom(name) ->
          "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"

        _other ->
          "exec_#{work.run_id}"
      end
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientFlakyPerAssetStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(execution_id, _timeout, opts) do
      counter = Keyword.fetch!(opts, :per_asset_counter)
      key = execution_id |> String.split("_") |> Enum.take(-2) |> Enum.join("_")

      attempt =
        Agent.get_and_update(counter, fn map ->
          current = Map.get(map, key, 0)
          next = current + 1
          {next, Map.put(map, key, next)}
        end)

      if attempt == 1 do
        {:ok,
         %RunnerResult{
           status: :error,
           error: :transient_failure,
           asset_results: [asset_result(execution_id, :error, :transient_failure)],
           metadata: %{attempt: attempt}
         }}
      else
        {:ok,
         %RunnerResult{
           status: :ok,
           asset_results: [asset_result(execution_id, :ok, nil)],
           metadata: %{attempt: attempt}
         }}
      end
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    defp asset_result(execution_id, status, error) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: error,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      case work.asset_ref do
        {module, name} when is_atom(module) and is_atom(name) ->
          "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"

        _other ->
          "exec_#{work.run_id}"
      end
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientTimeoutCancelableStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(_execution_id, _timeout, _opts), do: {:error, :timeout}

    @impl true
    def cancel_work(execution_id, reason, opts) do
      cancel_log = Keyword.fetch!(opts, :cancel_log)
      Agent.update(cancel_log, fn values -> [{execution_id, reason} | values] end)
      :ok
    end

    defp execution_id(work) do
      {module, name} = work.asset_ref
      "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"
    end
  end

  defmodule RunnerClientPartialSubmitStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts) do
      if work.asset_ref == {MyApp.Assets.Silver, :asset} do
        {:error, :submit_failed}
      else
        {:ok, execution_id(work)}
      end
    end

    @impl true
    def await_result(execution_id, _timeout, _opts) do
      {:ok,
       %RunnerResult{
         status: :ok,
         asset_results: [asset_result(execution_id, :ok)],
         metadata: %{stub: :partial_submit}
       }}
    end

    @impl true
    def cancel_work(execution_id, reason, opts) do
      cancel_log = Keyword.fetch!(opts, :cancel_log)
      Agent.update(cancel_log, fn values -> [{execution_id, reason} | values] end)
      :ok
    end

    defp asset_result(execution_id, status) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: nil,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      {module, name} = work.asset_ref
      "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientMetadataStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, execution_id(work)}

    @impl true
    def await_result(execution_id, _timeout, _opts) do
      {:ok,
       %RunnerResult{
         status: :ok,
         asset_results: [asset_result(execution_id, :ok)],
         metadata: %{runner_key: :runner_value}
       }}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    defp asset_result(execution_id, status) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: nil,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      {module, name} = work.asset_ref
      "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  defmodule RunnerClientRetryMetadataLeakStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, opts) do
      submit_log = Keyword.fetch!(opts, :submit_log)
      Agent.update(submit_log, fn values -> [work.metadata | values] end)
      {:ok, execution_id(work)}
    end

    @impl true
    def await_result(execution_id, _timeout, opts) do
      attempt_counter = Keyword.fetch!(opts, :attempt_counter)
      attempt = Agent.get_and_update(attempt_counter, fn value -> {value + 1, value + 1} end)

      if attempt == 1 do
        {:ok,
         %RunnerResult{
           status: :error,
           error: :transient_failure,
           asset_results: [asset_result(execution_id, :error, :transient_failure)],
           metadata: %{runner_key: :attempt_one}
         }}
      else
        {:ok,
         %RunnerResult{
           status: :ok,
           asset_results: [asset_result(execution_id, :ok, nil)],
           metadata: %{runner_key: :attempt_two}
         }}
      end
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    defp asset_result(execution_id, status, error) do
      ref = execution_ref(execution_id)

      %Favn.Run.AssetResult{
        ref: ref,
        stage: 0,
        status: status,
        started_at: DateTime.utc_now(),
        finished_at: DateTime.utc_now(),
        duration_ms: 0,
        meta: %{},
        error: error,
        attempt_count: 1,
        max_attempts: 1,
        attempts: []
      }
    end

    defp execution_id(work) do
      {module, name} = work.asset_ref
      "exec_#{work.run_id}_#{Atom.to_string(module)}_#{Atom.to_string(name)}"
    end

    defp execution_ref(execution_id) do
      [module, name] = execution_id |> String.split("_") |> Enum.take(-2)
      {String.to_atom(module), String.to_atom(name)}
    end
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    Memory.reset()

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "submits explicit manifest-pinned asset run and persists terminal state" do
    version = manifest_version("mv_run")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_run")

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               manifest_version_id: "mv_run",
               params: %{full_refresh: false},
               trigger: %{kind: :manual}
             )

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.manifest_version_id == "mv_run"
    assert run.asset_ref == {MyApp.Assets.Gold, :asset}
    assert run.plan.topo_order == [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Gold, :asset}]
    assert run.target_refs == [{MyApp.Assets.Gold, :asset}]
    assert run.result[:status] == :ok

    assert {:ok, events} = Storage.list_run_events(run_id)

    assert Enum.map(events, & &1.event_type) == [
             :run_created,
             :run_started,
             :step_started,
             :step_finished,
             :run_finished
           ]

    assert Enum.map(events, & &1.sequence) == [1, 2, 3, 4, 5]
  end

  test "accepted success transitions broadcast on both run and global topics in storage order" do
    version = manifest_version("mv_pubsub_success")
    run_id = "run_pubsub_success_#{System.unique_integer([:positive])}"

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    assert :ok = FavnOrchestrator.subscribe_run(run_id)
    assert :ok = FavnOrchestrator.subscribe_runs()

    assert {:ok, ^run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               run_id: run_id,
               manifest_version_id: version.manifest_version_id
             )

    assert {:ok, _run} = await_terminal_run(run_id)
    assert {:ok, stored_events} = Storage.list_run_events(run_id)

    received = collect_run_events_for_run(run_id, length(stored_events) * 2)

    assert Enum.frequencies_by(received, & &1.sequence) ==
             Map.new(stored_events, fn event -> {event.sequence, 2} end)

    assert Enum.sort_by(received, &{&1.sequence, &1.event_type})
           |> Enum.map(&{&1.sequence, &1.event_type})
           |> Enum.chunk_every(2)
           |> Enum.map(&List.first/1) == Enum.map(stored_events, &{&1.sequence, &1.event_type})
  end

  test "uses active manifest when manifest_version_id is not provided" do
    version = manifest_version("mv_active")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_active")

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset})
    assert {:ok, run} = await_terminal_run(run_id)
    assert run.manifest_version_id == "mv_active"
  end

  test "submits multi-target pipeline run in one run plan" do
    version = manifest_version("mv_pipeline_multi")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_pipeline_multi")

    assert {:ok, run_id} =
             FavnOrchestrator.submit_pipeline_run([
               {MyApp.Assets.Raw, :asset},
               {MyApp.Assets.Silver, :asset}
             ])

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.submit_kind == :pipeline
    assert run.target_refs == [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Silver, :asset}]
    assert run.plan.topo_order == [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Silver, :asset}]
    assert length(run.result.asset_results) == 2

    assert {:ok, events} = Storage.list_run_events(run_id)
    assert Enum.count(events, &(&1.event_type == :step_started)) == 2
    assert Enum.count(events, &(&1.event_type == :step_finished)) == 2
  end

  test "submits manual pipeline run from persisted manifest pipeline descriptor" do
    version = manifest_version("mv_pipeline_descriptor")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_pipeline_descriptor")

    assert {:ok, run_id} = FavnOrchestrator.submit_pipeline_run(MyApp.Pipelines.Daily)
    assert {:ok, run} = await_terminal_run(run_id)

    assert run.status == :ok
    assert run.submit_kind == :pipeline
    assert run.target_refs == [{MyApp.Assets.Gold, :asset}]
  end

  test "submits manual pipeline run from persisted descriptor with single-asset module shorthand" do
    version = manifest_version("mv_pipeline_single_asset_shorthand")
    assert :ok = FavnOrchestrator.register_manifest(version)

    assert :ok =
             FavnOrchestrator.activate_manifest("mv_pipeline_single_asset_shorthand")

    assert {:ok, run_id} =
             FavnOrchestrator.submit_pipeline_run(MyApp.Pipelines.SingleAssetShorthand)

    assert {:ok, run} = await_terminal_run(run_id)

    assert run.status == :ok
    assert run.submit_kind == :pipeline
    assert run.target_refs == [{MyApp.Assets.Gold, :asset}]
  end

  test "preserves orchestrator metadata and namespaces runner metadata on success paths" do
    version = manifest_version("mv_metadata_preserve")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_metadata_preserve")

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientMetadataStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    assert {:ok, source_run_id} = FavnOrchestrator.submit_pipeline_run(MyApp.Pipelines.Daily)
    assert {:ok, source_run} = await_terminal_run(source_run_id)

    assert source_run.metadata[:pipeline_context] == source_run.pipeline_context
    assert source_run.metadata[:pipeline_target_refs] == source_run.target_refs
    assert source_run.metadata[:pipeline_dependencies] == :all
    assert source_run.metadata[:pipeline_submit_ref] == MyApp.Pipelines.Daily
    assert source_run.metadata[:runner_metadata] == %{runner_key: :runner_value}
    refute Map.has_key?(source_run.metadata, :runner_key)

    assert {:ok, rerun_id} = FavnOrchestrator.rerun(source_run_id)
    assert {:ok, rerun} = await_terminal_run(rerun_id)

    assert rerun.metadata[:source_run_id] == source_run_id
    assert rerun.metadata[:replay_submit_kind] == :pipeline
    assert rerun.metadata[:replay_mode] == :exact_replay
    assert rerun.metadata[:pipeline_context] == source_run.pipeline_context
    assert rerun.metadata[:pipeline_submit_ref] == MyApp.Pipelines.Daily
    assert rerun.metadata[:runner_metadata] == %{runner_key: :runner_value}
  end

  test "sequential retries do not resend prior runner metadata back to the runner" do
    version = manifest_version("mv_retry_runner_metadata")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_retry_runner_metadata")

    {:ok, submit_log} = Agent.start_link(fn -> [] end)
    {:ok, attempt_counter} = Agent.start_link(fn -> 0 end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(submit_log) do
        Agent.stop(submit_log)
      end

      if Process.alive?(attempt_counter) do
        Agent.stop(attempt_counter)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientRetryMetadataLeakStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      submit_log: submit_log,
      attempt_counter: attempt_counter
    )

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               max_attempts: 2,
               retry_backoff_ms: 0
             )

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok

    [first_submit, second_submit] = submit_log |> Agent.get(&Enum.reverse(&1))
    refute Map.has_key?(first_submit, :runner_metadata)
    refute Map.has_key?(second_submit, :runner_metadata)
  end

  test "cancels multi-target pipeline run and forwards all in-flight execution ids" do
    version = manifest_version("mv_pipeline_cancel_multi")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_pipeline_cancel_multi")

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientSlowCancelableStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      cancel_log: cancel_log,
      block_ms: 250
    )

    assert {:ok, run_id} =
             FavnOrchestrator.submit_pipeline_run([
               {MyApp.Assets.Raw, :asset},
               {MyApp.Assets.Silver, :asset}
             ])

    assert {:ok, _in_flight_run} = await_inflight_run(run_id)

    assert :ok =
             FavnOrchestrator.cancel_run(run_id, %{
               requested_by: :operator,
               reason: :manual_cancel_pipeline
             })

    assert {:ok, cancelled} = await_cancelled_run(run_id)
    assert cancelled.status == :cancelled

    forwarded = Agent.get(cancel_log, & &1)
    assert forwarded != []
    assert Enum.any?(forwarded, fn {execution_id, _reason} -> is_binary(execution_id) end)
  end

  test "retries failed refs in stage-parallel pipeline mode" do
    version = manifest_version("mv_pipeline_retry_parallel")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_pipeline_retry_parallel")

    {:ok, per_asset_counter} = Agent.start_link(fn -> %{} end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(per_asset_counter) do
        Agent.stop(per_asset_counter)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientFlakyPerAssetStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      per_asset_counter: per_asset_counter
    )

    assert {:ok, run_id} =
             FavnOrchestrator.submit_pipeline_run(
               [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Silver, :asset}],
               max_attempts: 2,
               retry_backoff_ms: 0
             )

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert length(run.result.asset_results) >= 2
    assert map_size(run.asset_results) == 2

    assert {:ok, events} = Storage.list_run_events(run_id)
    assert Enum.count(events, &(&1.event_type == :step_retry_scheduled)) >= 2
    assert Enum.count(events, &(&1.event_type == :step_started)) >= 4
    assert Enum.count(events, &(&1.event_type == :step_finished)) >= 2
  end

  test "retries transient failures when max_attempts allows retries" do
    version = manifest_version("mv_retry")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_retry")

    {:ok, counter} = Agent.start_link(fn -> 0 end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(counter) do
        Agent.stop(counter)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientFlakyStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, attempt_counter: counter)

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               max_attempts: 2,
               retry_backoff_ms: 0
             )

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.event_seq >= 8

    assert {:ok, events} = Storage.list_run_events(run_id)

    assert Enum.member?(Enum.map(events, & &1.event_type), :step_retry_scheduled)
    assert Enum.count(events, &(&1.event_type == :step_started)) == 2
    assert Enum.map(events, & &1.sequence) == Enum.to_list(1..length(events))
  end

  test "retry transitions broadcast on both run and global topics in storage order" do
    version = manifest_version("mv_retry_pubsub")
    run_id = "run_retry_pubsub_#{System.unique_integer([:positive])}"

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(counter) do
        Agent.stop(counter)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientFlakyStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, attempt_counter: counter)

    assert :ok = FavnOrchestrator.subscribe_run(run_id)
    assert :ok = FavnOrchestrator.subscribe_runs()

    assert {:ok, ^run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               run_id: run_id,
               manifest_version_id: version.manifest_version_id,
               max_attempts: 2,
               retry_backoff_ms: 0
             )

    assert {:ok, _run} = await_terminal_run(run_id)
    assert {:ok, stored_events} = Storage.list_run_events(run_id)

    received = collect_run_events(length(stored_events) * 2)

    assert Enum.frequencies_by(received, & &1.sequence) ==
             Map.new(stored_events, fn event -> {event.sequence, 2} end)

    assert Enum.map(stored_events, & &1.sequence) == Enum.to_list(1..length(stored_events))
    assert Enum.member?(Enum.map(stored_events, & &1.event_type), :step_retry_scheduled)
  end

  test "cancels timed-out execution before retry/terminal transition" do
    version = manifest_version("mv_timeout_cancel")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_timeout_cancel")

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientTimeoutCancelableStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, cancel_log: cancel_log)

    assert {:ok, run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset}, max_attempts: 1)

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :timed_out

    assert {:ok, events} = Storage.list_run_events(run_id)
    assert Enum.map(events, & &1.sequence) == Enum.to_list(1..length(events))
    assert Enum.member?(Enum.map(events, & &1.event_type), :step_timed_out)
    assert Enum.member?(Enum.map(events, & &1.event_type), :run_timed_out)

    forwarded = Agent.get(cancel_log, & &1)
    assert length(forwarded) == 1
  end

  test "cancels already-submitted work when stage submit fails" do
    version = manifest_version("mv_partial_submit_cancel")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_partial_submit_cancel")

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientPartialSubmitStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, cancel_log: cancel_log)

    assert {:ok, run_id} =
             FavnOrchestrator.submit_pipeline_run([
               {MyApp.Assets.Raw, :asset},
               {MyApp.Assets.Silver, :asset}
             ])

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :error
    assert run.error == :submit_failed

    forwarded = Agent.get(cancel_log, & &1)
    assert forwarded != []
  end

  test "cancels in-flight run and forwards cancel to runner" do
    version = manifest_version("mv_cancel")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_cancel")

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientSlowCancelableStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      cancel_log: cancel_log,
      block_ms: 250
    )

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset})
    assert {:ok, in_flight_run} = await_inflight_run(run_id)
    assert is_binary(in_flight_run.runner_execution_id)

    assert :ok =
             FavnOrchestrator.cancel_run(run_id, %{
               requested_by: :operator,
               reason: :manual_cancel
             })

    assert {:ok, cancelled} = await_cancelled_run(run_id)
    assert cancelled.status == :cancelled

    assert {:ok, events} = Storage.list_run_events(run_id)
    assert Enum.member?(Enum.map(events, & &1.event_type), :run_cancel_requested)
    assert Enum.member?(Enum.map(events, & &1.event_type), :run_cancelled)
    assert Enum.map(events, & &1.sequence) == Enum.to_list(1..length(events))

    forwarded = Agent.get(cancel_log, & &1)
    assert length(forwarded) == 1

    [{execution_id, reason} | _] = forwarded
    assert execution_id == in_flight_run.runner_execution_id
    assert reason.reason[:reason] == :manual_cancel
  end

  test "cancel transitions broadcast on both run and global topics" do
    version = manifest_version("mv_cancel_pubsub")
    run_id = "run_cancel_pubsub_#{System.unique_integer([:positive])}"

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientSlowCancelableStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      cancel_log: cancel_log,
      block_ms: 250
    )

    assert {:ok, ^run_id} =
             FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset},
               run_id: run_id,
               manifest_version_id: version.manifest_version_id
             )

    assert {:ok, _in_flight_run} = await_inflight_run(run_id)

    assert :ok = FavnOrchestrator.subscribe_run(run_id)
    assert :ok = FavnOrchestrator.subscribe_runs()

    assert :ok =
             FavnOrchestrator.cancel_run(run_id, %{
               requested_by: :operator,
               reason: :manual_cancel_pubsub
             })

    assert {:ok, _cancelled} = await_cancelled_run(run_id)

    received = collect_run_events(4, 1_000)
    assert Enum.frequencies_by(received, & &1.event_type)[:run_cancel_requested] == 2
    assert Enum.frequencies_by(received, & &1.event_type)[:run_cancelled] == 2
  end

  test "rerun stays pinned to source manifest even when active manifest changes" do
    source_version = manifest_version("mv_source")
    newer_version = manifest_version("mv_newer")

    assert :ok = FavnOrchestrator.register_manifest(source_version)
    assert :ok = FavnOrchestrator.register_manifest(newer_version)

    assert :ok = FavnOrchestrator.activate_manifest("mv_source")
    assert {:ok, source_run_id} = FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset})
    assert {:ok, source_run} = await_terminal_run(source_run_id)
    assert source_run.manifest_version_id == "mv_source"

    assert :ok = FavnOrchestrator.activate_manifest("mv_newer")

    assert {:ok, rerun_id} = FavnOrchestrator.rerun(source_run_id)
    assert {:ok, rerun} = await_terminal_run(rerun_id)

    assert rerun.manifest_version_id == "mv_source"
    assert rerun.submit_kind == :rerun
    assert rerun.rerun_of_run_id == source_run_id
    assert rerun.parent_run_id == source_run_id
    assert rerun.root_run_id == source_run_id
    assert rerun.lineage_depth == source_run.lineage_depth + 1
  end

  test "pipeline rerun replays original target selection" do
    version = manifest_version("mv_rerun_pipeline_replay")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_rerun_pipeline_replay")

    assert {:ok, source_run_id} =
             FavnOrchestrator.submit_pipeline_run([
               {MyApp.Assets.Raw, :asset},
               {MyApp.Assets.Silver, :asset}
             ])

    assert {:ok, source_run} = await_terminal_run(source_run_id)
    assert source_run.status == :ok
    assert source_run.target_refs == [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Silver, :asset}]

    assert {:ok, rerun_id} = FavnOrchestrator.rerun(source_run_id)
    assert {:ok, rerun} = await_terminal_run(rerun_id)

    assert rerun.status == :ok
    assert rerun.target_refs == source_run.target_refs
    assert length(rerun.result.asset_results) == 2
  end

  test "pipeline-origin rerun keeps public pipeline projection and exact replay mode" do
    version = manifest_version("mv_pipeline_rerun_projection")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_pipeline_rerun_projection")

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientMetadataStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    assert {:ok, source_run_id} = FavnOrchestrator.submit_pipeline_run(MyApp.Pipelines.Daily)
    assert {:ok, source_run} = await_terminal_run(source_run_id)

    assert {:ok, rerun_id} = FavnOrchestrator.rerun(source_run_id)
    assert {:ok, rerun} = await_terminal_run(rerun_id)

    assert rerun.submit_kind == :rerun
    assert rerun.replay_mode == :exact_replay
    assert rerun.submit_ref == MyApp.Pipelines.Daily
    assert rerun.pipeline_context == source_run.pipeline_context
    assert rerun.pipeline != nil
    assert rerun.pipeline[:submit_ref] == MyApp.Pipelines.Daily
  end

  test "external cancel does not crash run server on stale write after await_result" do
    version = manifest_version("mv_cancel_stale_write")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_cancel_stale_write")

    {:ok, cancel_log} = Agent.start_link(fn -> [] end)

    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)

      if Process.alive?(cancel_log) do
        Agent.stop(cancel_log)
      end
    end)

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientSlowCancelableStub)

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      cancel_log: cancel_log,
      block_ms: 250
    )

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset})
    assert {:ok, _in_flight_run} = await_inflight_run(run_id)

    run_pid = :sys.get_state(FavnOrchestrator.RunManager).run_pids[run_id]
    ref = Process.monitor(run_pid)

    assert :ok =
             FavnOrchestrator.cancel_run(run_id, %{
               requested_by: :operator,
               reason: :manual_cancel
             })

    assert_receive {:DOWN, ^ref, :process, ^run_pid, :normal}, 1_000

    assert {:ok, cancelled} = await_cancelled_run(run_id)
    assert cancelled.status == :cancelled
  end

  test "rerun rejects manifest override mismatch" do
    source_version = manifest_version("mv_source_mismatch")
    other_version = manifest_version("mv_other_mismatch")

    assert :ok = FavnOrchestrator.register_manifest(source_version)
    assert :ok = FavnOrchestrator.register_manifest(other_version)
    assert :ok = FavnOrchestrator.activate_manifest("mv_source_mismatch")

    assert {:ok, source_run_id} = FavnOrchestrator.submit_asset_run({MyApp.Assets.Gold, :asset})
    assert {:ok, _source_run} = await_terminal_run(source_run_id)

    assert {:error, {:rerun_manifest_mismatch, "mv_source_mismatch", "mv_other_mismatch"}} =
             FavnOrchestrator.rerun(source_run_id, manifest_version_id: "mv_other_mismatch")
  end

  defp await_terminal_run(run_id, attempts \\ 30)

  defp await_terminal_run(run_id, attempts) when attempts > 0 do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status in [:ok, :error, :cancelled, :timed_out] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(20)
        await_terminal_run(run_id, attempts - 1)

      error ->
        error
    end
  end

  defp await_terminal_run(_run_id, 0), do: {:error, :timeout_waiting_for_terminal_state}

  defp await_inflight_run(run_id, attempts \\ 40)

  defp await_inflight_run(run_id, attempts) when attempts > 0 do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status == :running and is_binary(run.runner_execution_id) ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(15)
        await_inflight_run(run_id, attempts - 1)

      error ->
        error
    end
  end

  defp await_inflight_run(_run_id, 0), do: {:error, :timeout_waiting_for_inflight_state}

  defp await_cancelled_run(run_id, attempts \\ 40)

  defp await_cancelled_run(run_id, attempts) when attempts > 0 do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status == :cancelled ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(15)
        await_cancelled_run(run_id, attempts - 1)

      error ->
        error
    end
  end

  defp await_cancelled_run(_run_id, 0), do: {:error, :timeout_waiting_for_cancelled_state}

  defp collect_run_events(count, timeout \\ 2_000) when count > 0 do
    Enum.map(1..count, fn _index ->
      receive do
        {:favn_run_event, event} -> event
      after
        timeout -> flunk("expected #{count} pubsub run events")
      end
    end)
  end

  defp collect_run_events_for_run(run_id, count, timeout \\ 2_000)
       when is_binary(run_id) and count > 0 do
    collect_run_events_for_run(run_id, count, timeout, [])
  end

  defp collect_run_events_for_run(_run_id, 0, _timeout, acc), do: Enum.reverse(acc)

  defp collect_run_events_for_run(run_id, remaining, timeout, acc) do
    receive do
      {:favn_run_event, event} when event.run_id == run_id ->
        collect_run_events_for_run(run_id, remaining - 1, timeout, [event | acc])

      {:favn_run_event, _other_event} ->
        collect_run_events_for_run(run_id, remaining, timeout, acc)
    after
      timeout -> flunk("expected #{remaining} more pubsub run events for #{run_id}")
    end
  end

  defp manifest_version(manifest_version_id) do
    manifest =
      %Manifest{
        assets: [
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Raw, :asset},
            module: MyApp.Assets.Raw,
            name: :asset
          },
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Gold, :asset},
            module: MyApp.Assets.Gold,
            name: :asset,
            depends_on: [{MyApp.Assets.Raw, :asset}]
          },
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Silver, :asset},
            module: MyApp.Assets.Silver,
            name: :asset,
            depends_on: []
          }
        ],
        pipelines: [
          %Favn.Manifest.Pipeline{
            module: MyApp.Pipelines.Daily,
            name: :daily,
            selectors: [{:asset, {MyApp.Assets.Gold, :asset}}],
            deps: :all,
            schedule: nil,
            metadata: %{}
          },
          %Favn.Manifest.Pipeline{
            module: MyApp.Pipelines.SingleAssetShorthand,
            name: :single_asset_shorthand,
            selectors: [{:asset, MyApp.Assets.Gold}],
            deps: :all,
            schedule: nil,
            metadata: %{}
          }
        ]
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
