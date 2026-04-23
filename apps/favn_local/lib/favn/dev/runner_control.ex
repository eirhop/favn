defmodule Favn.Dev.RunnerControl do
  @moduledoc """
  Live runner control helpers for local tooling.

  This module talks to the running runner node via RPC instead of spawning
  one-off helper VMs.
  """

  alias Favn.Dev.NodeControl
  alias Favn.Manifest.Version

  @register_wait_timeout_ms 10_000
  @register_poll_interval_ms 100

  @type register_opt ::
          {:runner_node_name, String.t()}
          | {:rpc_cookie, String.t()}
          | {:runner_module, module()}
          | {:root_dir, Path.t()}

  @spec register_manifest(Version.t(), [register_opt()]) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts) do
    with {:ok, runner_node} <- fetch_runner_node(opts),
         {:ok, cookie} <- fetch_rpc_cookie(opts),
         :ok <- NodeControl.ensure_local_node_started(cookie),
         {:ok, runner_module} <- fetch_runner_module(opts),
         :ok <- ensure_connected(runner_node),
         {:ok, result} <-
           register_with_fallback(
             runner_node,
             version,
             runner_module,
             @register_wait_timeout_ms
           ) do
      case result do
        :ok -> :ok
        {:error, _reason} = error -> error
        other -> {:error, {:runner_register_unexpected, other}}
      end
    else
      {:error, {:runner_manifest_register_unavailable, _runner_node, _attempted}} = error -> error
      {:error, _reason} = error -> error
    end
  end

  defp fetch_runner_module(opts) do
    case Keyword.get(opts, :runner_module, FavnRunner) do
      module when is_atom(module) -> {:ok, module}
      _ -> {:error, :invalid_runner_module}
    end
  end

  defp register_with_fallback(runner_node, version, runner_module, timeout_ms)
       when is_integer(timeout_ms) and timeout_ms >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_register_with_fallback(runner_node, version, runner_module, deadline)
  end

  defp do_register_with_fallback(runner_node, version, runner_module, deadline_ms)
       when is_atom(runner_node) and is_atom(runner_module) do
    attempts = [
      {runner_module, :register_manifest, [version, []]},
      {runner_module, :register_manifest, [version]}
    ]

    case try_register_attempts(runner_node, attempts) do
      {:ok, result} ->
        {:ok, result}

      :unavailable ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:error,
           {:runner_manifest_register_unavailable, runner_node, attempts_to_metadata(attempts)}}
        else
          Process.sleep(@register_poll_interval_ms)
          do_register_with_fallback(runner_node, version, runner_module, deadline_ms)
        end
    end
  end

  defp try_register_attempts(runner_node, attempts) when is_list(attempts) do
    Enum.reduce_while(attempts, :unavailable, fn attempt, :unavailable ->
      case invoke_register_attempt(runner_node, attempt) do
        :unavailable -> {:cont, :unavailable}
        {:ok, result} -> {:halt, {:ok, result}}
      end
    end)
  end

  defp invoke_register_attempt(runner_node, {module, function, args})
       when is_atom(runner_node) and is_atom(module) and is_atom(function) and is_list(args) do
    {:ok, :erpc.call(runner_node, module, function, args, 10_000)}
  rescue
    error in ErlangError ->
      if remote_startup_unavailable?(error.original, module, function, length(args)) do
        :unavailable
      else
        reraise error, __STACKTRACE__
      end
  catch
    :exit, reason ->
      if remote_startup_unavailable?(reason, module, function, length(args)) do
        :unavailable
      else
        :erlang.raise(:exit, reason, __STACKTRACE__)
      end
  end

  defp attempts_to_metadata(attempts) when is_list(attempts) do
    Enum.map(attempts, fn {module, function, args} ->
      %{module: module, function: function, arity: length(args)}
    end)
  end

  defp remote_startup_unavailable?(reason, module, function, arity) do
    remote_undef?(reason, module, function, arity) or remote_noproc?(reason) or
      remote_noconnection?(reason)
  end

  defp remote_undef?({:exception, :undef, stacktrace}, module, function, arity)
       when is_list(stacktrace) do
    Enum.any?(stacktrace, fn
      {^module, ^function, args, _location} when is_list(args) -> length(args) == arity
      {^module, ^function, ^arity, _location} -> true
      _other -> false
    end)
  end

  defp remote_undef?(_reason, _module, _function, _arity), do: false

  defp remote_noproc?({:exception, {:noproc, _details}}), do: true
  defp remote_noproc?({:noproc, _details}), do: true
  defp remote_noproc?(_reason), do: false

  defp remote_noconnection?({:erpc, :noconnection}), do: true
  defp remote_noconnection?(_reason), do: false

  defp fetch_runner_node(opts) do
    case Keyword.get(opts, :runner_node_name) do
      value when is_binary(value) and value != "" -> {:ok, String.to_atom(value)}
      _ -> {:error, :missing_runner_node_name}
    end
  end

  defp fetch_rpc_cookie(opts) do
    case Keyword.get(opts, :rpc_cookie) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :missing_rpc_cookie}
    end
  end

  defp ensure_connected(runner_node) do
    case Node.connect(runner_node) do
      true -> :ok
      false -> {:error, {:runner_node_unreachable, runner_node}}
      :ignored -> {:error, {:runner_node_ignored, runner_node}}
    end
  end
end
