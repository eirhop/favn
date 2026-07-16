defmodule FavnRunner.ExecutionAdmission do
  @moduledoc """
  Runner-owned admission contract for bounded worker concurrency.

  The first admission slice enforces active worker capacity. Queue configuration
  is normalized and exposed in diagnostics, but work is rejected instead of
  silently blocking when active capacity is exhausted.
  """

  alias Favn.Contracts.RunnerError

  @default_queue_timeout_ms 30_000

  @type t :: %__MODULE__{
          max_active_workers: pos_integer(),
          max_queue_size: non_neg_integer(),
          queue_timeout_ms: pos_integer(),
          rejected_overload_count: non_neg_integer()
        }

  defstruct max_active_workers: 1,
            max_queue_size: 2,
            queue_timeout_ms: @default_queue_timeout_ms,
            rejected_overload_count: 0

  @doc """
  Builds normalized admission state from runner options or application env.
  """
  @spec new(keyword() | map()) :: t()
  def new(opts \\ []) when is_list(opts) or is_map(opts) do
    admission = option_value(opts, :admission, Application.get_env(:favn_runner, :admission, []))

    max_active_workers =
      positive_value(admission, :max_active_workers, default_max_active_workers())

    %__MODULE__{
      max_active_workers: max_active_workers,
      max_queue_size: non_negative_value(admission, :max_queue_size, max_active_workers * 2),
      queue_timeout_ms: positive_value(admission, :queue_timeout_ms, @default_queue_timeout_ms)
    }
  end

  @doc """
  Admits work when active worker capacity is available.
  """
  @spec admit(t(), non_neg_integer()) :: {:ok, t()} | {:error, RunnerError.t(), t()}
  def admit(%__MODULE__{} = admission, active_worker_count)
      when is_integer(active_worker_count) and active_worker_count >= 0 do
    if active_worker_count < admission.max_active_workers do
      {:ok, admission}
    else
      admission = %{admission | rejected_overload_count: admission.rejected_overload_count + 1}

      {:error,
       RunnerError.normalize(:runner_overloaded,
         kind: :boundary,
         type: :runner_overloaded,
         message: "Runner worker capacity is exhausted",
         details: %{
           active_worker_count: active_worker_count,
           max_active_workers: admission.max_active_workers,
           max_queue_size: admission.max_queue_size
         },
         retryable?: true,
         outcome: :safe_failure
       ), admission}
    end
  end

  @doc """
  Returns admission diagnostics.
  """
  @spec diagnostics(t(), non_neg_integer(), non_neg_integer()) :: map()
  def diagnostics(%__MODULE__{} = admission, active_worker_count, queued_worker_count)
      when is_integer(active_worker_count) and active_worker_count >= 0 and
             is_integer(queued_worker_count) and queued_worker_count >= 0 do
    %{
      active_worker_count: active_worker_count,
      queued_worker_count: queued_worker_count,
      rejected_overload_count: admission.rejected_overload_count,
      max_active_workers: admission.max_active_workers,
      max_queue_size: admission.max_queue_size,
      queue_timeout_ms: admission.queue_timeout_ms
    }
  end

  defp default_max_active_workers do
    System.schedulers_online()
    |> max(1)
  end

  defp positive_value(config, key, default) do
    case config_value(config, key, default) do
      value when is_integer(value) and value > 0 -> value
      _other -> default
    end
  end

  defp non_negative_value(config, key, default) do
    case config_value(config, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _other -> default
    end
  end

  defp config_value(config, key, default) when is_list(config),
    do: Keyword.get(config, key, default)

  defp config_value(config, key, default) when is_map(config), do: Map.get(config, key, default)
  defp config_value(_config, _key, default), do: default

  defp option_value(opts, key, default) when is_list(opts), do: Keyword.get(opts, key, default)
  defp option_value(opts, key, default) when is_map(opts), do: Map.get(opts, key, default)
end
