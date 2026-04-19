defmodule Favn.Dev do
  @moduledoc """
  Local developer tooling facade.

  This module is intentionally small and delegates to focused implementation
  modules under `Favn.Dev.*`.
  """

  alias Favn.Dev.Reload
  alias Favn.Dev.Stack
  alias Favn.Dev.Status

  @type status_opts :: [root_dir: Path.t()]
  @type lifecycle_opts :: [root_dir: Path.t()]

  @doc """
  Starts local stack in foreground mode.
  """
  @spec dev(lifecycle_opts()) :: :ok | {:error, term()}
  def dev(opts \\ []) when is_list(opts), do: Stack.start_foreground(opts)

  @doc """
  Stops local stack using project-local runtime metadata.
  """
  @spec stop(lifecycle_opts()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts), do: Stack.stop(opts)

  @doc """
  Rebuilds and republishes the manifest to the running local orchestrator.
  """
  @spec reload(lifecycle_opts()) :: :ok | {:error, term()}
  def reload(opts \\ []) when is_list(opts), do: Reload.run(opts)

  @doc """
  Returns local stack status for the current project.
  """
  @spec status(status_opts()) :: map()
  def status(opts \\ []) when is_list(opts), do: Status.inspect_stack(opts)
end
