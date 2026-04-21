defmodule Favn.Dev.Reset do
  @moduledoc """
  Destructive project-local cleanup for `.favn/` state and artifacts.
  """

  alias Favn.Dev.Lock
  alias Favn.Dev.Paths
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.State

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: :ok | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    Lock.with_lock(opts, fn ->
      root_dir = Paths.root_dir(opts)
      favn_dir = Paths.favn_dir(root_dir)

      with :ok <- ensure_stack_stopped(opts), do: remove_favn_dir(favn_dir)
    end)
  end

  defp ensure_stack_stopped(opts) do
    case State.read_runtime(opts) do
      {:ok, runtime} ->
        if any_service_running?(runtime) do
          {:error, :stack_running}
        else
          :ok
        end

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp any_service_running?(runtime) do
    runtime
    |> Map.get("services", %{})
    |> Map.values()
    |> Enum.any?(fn
      %{"pid" => pid} when is_integer(pid) and pid > 0 -> DevProcess.alive?(pid)
      _ -> false
    end)
  end

  defp remove_favn_dir(path) do
    case File.rm_rf(path) do
      {:ok, _entries} -> :ok
      {:error, reason, _path} -> {:error, {:reset_failed, reason}}
    end
  end
end
