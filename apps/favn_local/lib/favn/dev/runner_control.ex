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
         runner_module <- Keyword.get(opts, :runner_module, FavnRunner),
         true <- is_atom(runner_module),
         :ok <- ensure_connected(runner_node) do
      case :erpc.call(runner_node, runner_module, :register_manifest, [version, []], 10_000) do
        :ok -> :ok
        {:error, _reason} = error -> error
        other -> {:error, {:runner_register_unexpected, other}}
      end
    else
      false -> {:error, :invalid_runner_module}
      {:error, _reason} = error -> error
    end
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
