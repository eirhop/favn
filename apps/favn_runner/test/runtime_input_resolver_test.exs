defmodule FavnRunner.RuntimeInputResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Run.Context
  alias Favn.RuntimeInputResolver.Ref
  alias Favn.SQLAsset.Error
  alias Favn.SQLAsset.RuntimeInputs.Error, as: ResolverError
  alias Favn.SQLAsset.RuntimeInputs.Result
  alias FavnRunner.RuntimeInputResolver
  alias FavnRunner.RuntimeInputResolver.Resolution

  defmodule SuccessResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(context) do
      send(context.runtime_config.test_pid, {:resolver_context, context})

      {:ok,
       %Result{
         params: %{snapshot_id: "secret-snapshot", expected_count: 12},
         identity: "manifest:12",
         metadata: %{version: 3, files: ["a", "b"], accidental_echo: "secret-snapshot"},
         sensitive_params: [:snapshot_id]
       }}
    end
  end

  defmodule ExplicitErrorResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context) do
      {:error,
       %ResolverError{
         reason: :source_unavailable,
         message: "source manifest is unavailable",
         retryable?: true,
         metadata: %{source: "orders"}
       }}
    end
  end

  defmodule SettingsResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(context) do
      {:ok,
       %Result{
         params: %{source: context.asset.settings.source},
         identity: "source:#{context.asset.settings.source}"
       }}
    end
  end

  defmodule RaiseResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context), do: raise("must not be surfaced")
  end

  defmodule ExitResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context), do: exit(:resolver_stopped)
  end

  defmodule InvalidResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context), do: {:ok, %{params: %{id: 1}}}
  end

  defmodule SleepResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context) do
      Process.sleep(1_000)
      {:ok, %Result{params: %{}, identity: "late"}}
    end
  end

  defmodule BlockingResolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(context) do
      send(context.runtime_config.test_pid, {:blocking_resolver_started, self()})

      receive do
        :stop -> {:ok, %Result{params: %{}, identity: "stopped"}}
      end
    end
  end

  defmodule MissingCallbackResolver do
  end

  test "resolves, validates, merges, emits safe lineage, and redacts sensitive values" do
    attach_runtime_input_telemetry()
    context = context()

    assert {:ok, %Resolution{} = resolution} =
             resolve(SuccessResolver, context, %{submitted: "normal"})

    assert_received {:resolver_context, ^context}

    assert resolution.params == %{
             submitted: "normal",
             snapshot_id: "secret-snapshot",
             expected_count: 12
           }

    assert RuntimeInputResolver.lineage(resolution) == %{
             resolver: SuccessResolver,
             input_identity: "manifest:12",
             input_metadata: %{
               version: 3,
               files: ["a", "b"],
               accidental_echo: :redacted
             },
             duration_ms: resolution.duration_ms
           }

    redacted =
      RuntimeInputResolver.redact(
        %Error{
          type: :backend_execution_failed,
          phase: :materialize,
          message: "adapter rejected secret-snapshot",
          details: %{value: "secret-snapshot"}
        },
        resolution
      )

    assert redacted.message == "adapter rejected [REDACTED]"
    assert redacted.details.value == :redacted

    assert_receive {:runtime_input_telemetry, %{duration_ms: duration_ms}, metadata}
    assert duration_ms >= 0
    assert metadata.resolver == SuccessResolver
    assert metadata.outcome == :ok
    refute inspect(metadata) =~ "secret-snapshot"
  end

  test "resolver code can reuse static asset settings" do
    context = put_in(context().asset.settings, %{source: "orders"})

    assert {:ok, %Resolution{} = resolution} = resolve(SettingsResolver, context)
    assert resolution.params == %{source: "orders"}
    assert resolution.identity == "source:orders"
  end

  test "rejects submitted collisions, reserved names, invalid types, and unknown sensitive names" do
    context = context()

    assert {:error, %{type: :runtime_inputs_param_collision}} =
             resolve(SuccessResolver, context, %{"snapshot_id" => "submitted"})

    assert {:error, %{type: :runtime_inputs_param_collision}} =
             resolve_result(%Result{params: %{window_start: 1}, identity: "reserved"}, context)

    assert {:error, %{type: :runtime_inputs_param_collision}} =
             resolve_result(
               %Result{params: %{favn_run_id: "override"}, identity: "reserved"},
               context
             )

    assert {:error, %{type: :runtime_inputs_invalid_result}} =
             resolve_result(%Result{params: %{bad: self()}, identity: "bad"}, context)

    assert {:error, %{type: :runtime_inputs_invalid_result}} =
             resolve_result(
               %Result{
                 params: %{present: 1},
                 identity: "bad-sensitive-name",
                 sensitive_params: [:missing]
               },
               context
             )
  end

  test "preserves typed resolver errors without enabling a retry policy" do
    assert {:error, %Error{} = error} = resolve(ExplicitErrorResolver, context())
    assert error.type == :runtime_inputs_failed
    assert error.message == "source manifest is unavailable"
    assert error.details.reason == :source_unavailable
    assert error.details.resolver_metadata == %{source: "orders"}
    assert error.details.asset_retryable? == true
  end

  test "redacts an input identity that repeats a sensitive parameter value" do
    assert {:ok, resolution} =
             resolve_result(
               %Result{
                 params: %{token: "shared-secret"},
                 identity: "shared-secret",
                 sensitive_params: [:token]
               },
               context()
             )

    assert RuntimeInputResolver.lineage(resolution).input_identity == "[REDACTED]"
  end

  test "normalizes raises, exits, invalid returns, and timeouts without leaking terms" do
    assert {:error, %Error{type: :runtime_inputs_raised} = raised} =
             resolve(RaiseResolver, context())

    refute inspect(raised) =~ "must not be surfaced"

    assert {:error, %Error{type: :runtime_inputs_raised}} = resolve(ExitResolver, context())

    assert {:error, %Error{type: :runtime_inputs_invalid_result}} =
             resolve(InvalidResolver, context())

    started_at = System.monotonic_time(:millisecond)

    assert {:error, %Error{type: :runtime_inputs_timeout}} =
             resolve(SleepResolver, context(), %{}, timeout_ms: 10)

    assert System.monotonic_time(:millisecond) - started_at < 500
  end

  test "rejects missing runtime modules and callbacks precisely" do
    missing_module = Module.concat(__MODULE__, "Missing#{System.unique_integer([:positive])}")

    assert {:error, %Error{type: :runtime_inputs_missing_module}} =
             resolve(missing_module, context())

    assert {:error, %Error{type: :runtime_inputs_missing_callback}} =
             resolve(MissingCallbackResolver, context())
  end

  test "enforces parameter, identity, and metadata budgets before rendering" do
    too_many_params = Map.new(1..129, &{"param_#{&1}", &1})

    assert {:error, %Error{type: :runtime_inputs_payload_too_large}} =
             resolve_result(%Result{params: too_many_params, identity: "many"}, context())

    assert {:error, %Error{type: :runtime_inputs_payload_too_large}} =
             resolve_result(
               %Result{
                 params: %{payload: String.duplicate("x", 4 * 1_024 * 1_024)},
                 identity: "large"
               },
               context()
             )

    assert {:error, %Error{type: :runtime_inputs_invalid_result}} =
             resolve_result(
               %Result{params: %{}, identity: String.duplicate("i", 1_025)},
               context()
             )

    assert {:error, %Error{type: :runtime_inputs_invalid_result}} =
             resolve_result(
               %Result{params: %{}, identity: "bad-metadata", metadata: %{pid: self()}},
               context()
             )

    too_many_metadata_entries = Map.new(1..129, &{"entry_#{&1}", &1})

    assert {:error, %Error{type: :runtime_inputs_payload_too_large}} =
             resolve_result(
               %Result{
                 params: %{},
                 identity: "large-metadata",
                 metadata: too_many_metadata_entries
               },
               context()
             )
  end

  test "cancelling the lifecycle owner deterministically stops resolver code" do
    attach_runtime_input_telemetry()
    test_pid = self()
    context = context()

    caller =
      spawn(fn ->
        result = resolve(BlockingResolver, context)
        send(test_pid, {:unexpected_resolution, result})
      end)

    caller_ref = Process.monitor(caller)
    assert_receive {:blocking_resolver_started, resolver_pid}
    resolver_ref = Process.monitor(resolver_pid)

    Process.exit(caller, :kill)

    assert_receive {:DOWN, ^caller_ref, :process, ^caller, :killed}
    assert_receive {:DOWN, ^resolver_ref, :process, ^resolver_pid, :killed}, 500

    assert_receive {:runtime_input_telemetry, _measurements,
                    %{outcome: :runtime_inputs_cancelled}}

    refute_received {:unexpected_resolution, _result}
  end

  defp resolve(module, context, submitted_params \\ %{}, opts \\ []) do
    RuntimeInputResolver.resolve(%Ref{module: module}, context, submitted_params, opts)
  end

  defp resolve_result(%Result{} = result, context) do
    module = Module.concat(__MODULE__, "Dynamic#{System.unique_integer([:positive])}")

    {:module, ^module, _binary, _exports} =
      Module.create(
        module,
        quote do
          @behaviour Favn.SQLAsset.RuntimeInputs
          @impl true
          def resolve(_context), do: {:ok, unquote(Macro.escape(result))}
        end,
        Macro.Env.location(__ENV__)
      )

    resolve(module, context)
  end

  defp context do
    %Context{
      run_id: "run-runtime-inputs",
      target_refs: [{__MODULE__, :asset}],
      asset: %Favn.Run.AssetContext{ref: {__MODULE__, :asset}, relation: nil, settings: %{}},
      runtime_config: %{test_pid: self()},
      params: %{},
      window: nil,
      pipeline: nil,
      run_started_at: DateTime.utc_now(),
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }
  end

  defp attach_runtime_input_telemetry do
    handler_id = "#{inspect(__MODULE__)}-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :sql_asset, :runtime_inputs],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:runtime_input_telemetry, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
