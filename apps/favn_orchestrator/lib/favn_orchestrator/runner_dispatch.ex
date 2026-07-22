defmodule FavnOrchestrator.RunnerDispatch do
  @moduledoc """
  Applies the control-plane lifecycle boundary to calls that start runner work.

  A call that acquires a permit before draining may finish. Once draining has
  begun, no new execution, runtime-input resolution, or executable inspection
  is sent to the runner.
  """

  alias FavnOrchestrator.Lifecycle

  @doc "Submits execution work while holding a control-plane admission permit."
  @spec submit_work(module(), term(), keyword(), GenServer.server()) :: term()
  def submit_work(runner_client, work, runner_opts, lifecycle \\ Lifecycle)
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.submit_work(work, runner_opts) end,
      lifecycle
    )
  end

  @doc "Resolves runtime inputs while holding a control-plane admission permit."
  @spec resolve_runtime_inputs(module(), term(), keyword(), GenServer.server()) :: term()
  def resolve_runtime_inputs(runner_client, work, runner_opts, lifecycle \\ Lifecycle)
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.resolve_runtime_inputs(work, runner_opts) end,
      lifecycle
    )
  end

  @doc "Runs executable relation inspection under a control-plane admission permit."
  @spec inspect_relation(module(), term(), keyword(), GenServer.server()) :: term()
  def inspect_relation(runner_client, request, runner_opts, lifecycle \\ Lifecycle)
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.inspect_relation(request, runner_opts) end,
      lifecycle
    )
  end

  @doc "Reads generation capabilities under a control-plane admission permit."
  @spec generation_capabilities(module(), term(), Favn.Ref.t(), keyword(), GenServer.server()) ::
          term()
  def generation_capabilities(
        runner_client,
        version,
        asset_ref,
        runner_opts,
        lifecycle \\ Lifecycle
      )
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.generation_capabilities(version, asset_ref, runner_opts) end,
      lifecycle
    )
  end

  @doc "Reads one generation marker under a control-plane admission permit."
  @spec generation_marker(module(), term(), Favn.Ref.t(), keyword(), GenServer.server()) :: term()
  def generation_marker(runner_client, version, asset_ref, runner_opts, lifecycle \\ Lifecycle)
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.generation_marker(version, asset_ref, runner_opts) end,
      lifecycle
    )
  end

  @doc "Initializes one generation marker under a control-plane admission permit."
  @spec initialize_generation_marker(module(), term(), keyword(), GenServer.server()) :: term()
  def initialize_generation_marker(
        runner_client,
        request,
        runner_opts,
        lifecycle \\ Lifecycle
      )
      when is_atom(runner_client) and is_list(runner_opts) do
    Lifecycle.with_admission(
      fn -> runner_client.initialize_generation_marker(request, runner_opts) end,
      lifecycle
    )
  end
end
