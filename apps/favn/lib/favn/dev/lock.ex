defmodule Favn.Dev.Lock do
  @moduledoc """
  File-based lock used to serialize `.favn/` lifecycle mutations.
  """

  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @type root_opt :: [root_dir: Path.t()]

  @doc """
  Runs `fun` under an exclusive `.favn/lock` directory lock.
  """
  @spec with_lock(root_opt(), (-> term())) :: term()
  def with_lock(opts \\ [], fun) when is_list(opts) and is_function(fun, 0) do
    with :ok <- State.ensure_layout(opts),
         :ok <- acquire_lock(opts, 100) do
      try do
        fun.()
      after
        release_lock(opts)
      end
    end
  end

  @spec acquire_lock(root_opt(), non_neg_integer()) :: :ok | {:error, term()}
  defp acquire_lock(_opts, 0), do: {:error, {:lock_failed, :timeout}}

  defp acquire_lock(opts, attempts_left) do
    path = opts |> Paths.root_dir() |> Paths.lock_path()

    case File.mkdir(path) do
      :ok ->
        :ok

      {:error, :eexist} ->
        Process.sleep(50)
        acquire_lock(opts, attempts_left - 1)

      {:error, reason} ->
        {:error, {:lock_failed, reason}}
    end
  end

  @spec release_lock(root_opt()) :: :ok
  defp release_lock(opts) do
    path = opts |> Paths.root_dir() |> Paths.lock_path()

    case File.rmdir(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, _reason} -> :ok
    end
  end
end
