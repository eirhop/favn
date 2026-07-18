defmodule FavnOrchestrator.RunManager.PlanCapacity do
  @moduledoc false

  alias FavnOrchestrator.RunState

  @default_max_bytes 512 * 1_024 * 1_024
  @term_budget_multiplier 4

  @enforce_keys [:max_bytes]
  defstruct max_bytes: @default_max_bytes, allocated_bytes: 0, allocations: %{}

  @type key :: {String.t(), String.t()}
  @type t :: %__MODULE__{
          max_bytes: pos_integer(),
          allocated_bytes: non_neg_integer(),
          allocations: %{optional(key()) => non_neg_integer()}
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    max_bytes =
      Keyword.get_lazy(opts, :max_active_run_plan_bytes, fn ->
        Application.get_env(
          :favn_orchestrator,
          :active_run_plan_max_bytes,
          @default_max_bytes
        )
      end)

    %__MODULE__{max_bytes: validate_max_bytes!(max_bytes)}
  end

  @spec validate_run(t(), RunState.t()) :: :ok | {:error, term()}
  def validate_run(%__MODULE__{max_bytes: max_bytes}, %RunState{} = run) do
    bytes = allocation_bytes(run)

    if bytes <= max_bytes,
      do: :ok,
      else: {:error, {:run_plan_exceeds_node_capacity, bytes, max_bytes}}
  end

  @spec reserve(t(), key(), RunState.t()) ::
          {:ok, t()} | {:error, {:run_plan_capacity_exhausted, map()}}
  def reserve(%__MODULE__{} = capacity, key, %RunState{} = run) do
    bytes = allocation_bytes(run)

    cond do
      Map.has_key?(capacity.allocations, key) ->
        {:ok, capacity}

      capacity.allocated_bytes + bytes <= capacity.max_bytes ->
        {:ok,
         %{
           capacity
           | allocated_bytes: capacity.allocated_bytes + bytes,
             allocations: Map.put(capacity.allocations, key, bytes)
         }}

      true ->
        {:error,
         {:run_plan_capacity_exhausted,
          %{
            required_bytes: bytes,
            allocated_bytes: capacity.allocated_bytes,
            max_bytes: capacity.max_bytes
          }}}
    end
  end

  @spec release(t(), key()) :: t()
  def release(%__MODULE__{} = capacity, key) do
    case Map.pop(capacity.allocations, key) do
      {nil, allocations} ->
        %{capacity | allocations: allocations}

      {bytes, allocations} ->
        %{
          capacity
          | allocated_bytes: max(capacity.allocated_bytes - bytes, 0),
            allocations: allocations
        }
    end
  end

  @spec allocation_bytes(RunState.t()) :: non_neg_integer()
  def allocation_bytes(%RunState{plan: nil}), do: 0

  def allocation_bytes(%RunState{plan: plan}) do
    @term_budget_multiplier * :erlang.external_size(plan)
  end

  @spec diagnostics(t()) :: map()
  def diagnostics(%__MODULE__{} = capacity) do
    %{
      max_bytes: capacity.max_bytes,
      allocated_bytes: capacity.allocated_bytes,
      available_bytes: capacity.max_bytes - capacity.allocated_bytes,
      active_run_count: map_size(capacity.allocations)
    }
  end

  defp validate_max_bytes!(value) when is_integer(value) and value > 0, do: value

  defp validate_max_bytes!(value) do
    raise ArgumentError,
          ":max_active_run_plan_bytes must be a positive integer, got: #{inspect(value)}"
  end
end
