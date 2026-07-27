defmodule Favn.Deployment.RunnerPoolSpec do
  @moduledoc """
  Provider-neutral deployment inputs for one exact runner pool/release.

  CPU, memory, image, price, and provider identifiers intentionally remain
  deployment-owned values.
  """

  @enforce_keys [
    :runner_pool,
    :runner_release_id,
    :lifecycle_mode,
    :max_runners,
    :polling_seconds,
    :runner_max_uptime_seconds,
    :max_task_seconds,
    :shutdown_grace_seconds
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    spec = struct(__MODULE__, attrs)

    with :ok <- Favn.RunnerPool.validate_runtime(spec.runner_pool),
         :ok <- Favn.RunnerRelease.validate_id(spec.runner_release_id),
         true <- spec.lifecycle_mode in [:elastic, :resident],
         :ok <- positive(:max_runners, spec.max_runners, 10_000),
         :ok <- positive(:polling_seconds, spec.polling_seconds, 300),
         :ok <- positive(:runner_max_uptime_seconds, spec.runner_max_uptime_seconds, 604_800),
         :ok <- positive(:max_task_seconds, spec.max_task_seconds, 604_800),
         :ok <- positive(:shutdown_grace_seconds, spec.shutdown_grace_seconds, 3_600) do
      {:ok, spec}
    else
      false -> {:error, {:invalid_lifecycle_mode, spec.lifecycle_mode}}
      {:error, _reason} = error -> error
    end
  rescue
    KeyError -> {:error, :missing_runner_pool_deployment_field}
  end

  def new(_attrs), do: {:error, :invalid_runner_pool_deployment}

  @doc "Minimum infrastructure hard timeout that cannot race normal runner drain."
  @spec minimum_hard_timeout_seconds(t()) :: pos_integer()
  def minimum_hard_timeout_seconds(%__MODULE__{} = spec) do
    spec.runner_max_uptime_seconds + spec.max_task_seconds + spec.shutdown_grace_seconds
  end

  @doc "Exact stable capacity endpoint for this immutable partition."
  @spec demand_path(t()) :: String.t()
  def demand_path(%__MODULE__{} = spec),
    do: "/internal/runner-demand/#{spec.runner_pool}/#{spec.runner_release_id}"

  defp positive(_field, value, maximum)
       when is_integer(value) and value > 0 and value <= maximum,
       do: :ok

  defp positive(field, value, _maximum), do: {:error, {:invalid_deployment_field, field, value}}
end
