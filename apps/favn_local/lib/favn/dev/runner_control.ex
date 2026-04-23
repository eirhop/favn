defmodule Favn.Dev.RunnerControl do
  @moduledoc """
  Live runner control helpers for local tooling.

  This module talks to the running runner node via RPC instead of spawning
  one-off helper VMs.
  """

  alias Favn.Dev.NodeControl
  alias Favn.Manifest.Version

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
         {:ok, {module, function, args}, _attempted} <-
             resolve_register_entrypoint(runner_node, version, runner_module) do
       case :erpc.call(runner_node, module, function, args, 10_000) do
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

  defp resolve_register_entrypoint(runner_node, version, runner_module)
       when is_atom(runner_node) and is_atom(runner_module) do
    attempts = [
      {runner_module, :register_manifest, [version, []]},
      {runner_module, :register_manifest, [version]}
    ]

    case Enum.find(attempts, &remote_exported?(runner_node, &1)) do
      {module, function, args} ->
        {:ok, {module, function, args}, attempts_to_metadata(attempts)}

      nil ->
        {:error,
         {:runner_manifest_register_unavailable, runner_node, attempts_to_metadata(attempts)}}
    end
  end

  defp remote_exported?(runner_node, {module, function, args})
       when is_atom(runner_node) and is_atom(module) and is_atom(function) and is_list(args) do
    arity = length(args)

    case :erpc.call(runner_node, :erlang, :function_exported, [module, function, arity], 2_000) do
      true -> true
      false -> false
      _other -> false
    end
  rescue
    ErlangError -> false
  end

  defp attempts_to_metadata(attempts) when is_list(attempts) do
    Enum.map(attempts, fn {module, function, args} ->
      %{module: module, function: function, arity: length(args)}
    end)
  end

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
