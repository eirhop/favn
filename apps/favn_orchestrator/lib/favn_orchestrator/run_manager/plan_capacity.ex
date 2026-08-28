defmodule FavnOrchestrator.RunManager.PlanCapacity do
  @moduledoc false

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Error, as: MemoryError

  @default_max_bytes 512 * 1_024 * 1_024

  @enforce_keys [:max_bytes]
  defstruct max_bytes: @default_max_bytes,
            allocated_bytes: 0,
            allocations: %{},
            memory_tokens: %{},
            memory_server: Coordinator,
            coordinate_memory?: true

  @type key :: {String.t(), String.t()}
  @type t :: %__MODULE__{
          max_bytes: pos_integer(),
          allocated_bytes: non_neg_integer(),
          allocations: %{optional(key()) => non_neg_integer()},
          memory_tokens: %{optional(key()) => MemoryCapacity.t()},
          memory_server: GenServer.server(),
          coordinate_memory?: boolean()
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

    %__MODULE__{
      max_bytes: validate_max_bytes!(max_bytes),
      memory_server: Keyword.get(opts, :memory_server, Coordinator),
      coordinate_memory?:
        Keyword.get(
          opts,
          :coordinate_memory,
          Application.get_env(:favn_orchestrator, :start_runtime, true)
        )
    }
  end

  @spec validate_run(t(), RunState.t()) :: :ok | {:error, term()}
  def validate_run(%__MODULE__{max_bytes: max_bytes}, %RunState{} = run) do
    bytes = allocation_bytes(run)

    if bytes <= max_bytes,
      do: :ok,
      else: {:error, {:run_plan_exceeds_node_capacity, bytes, max_bytes}}
  end

  @spec reserve(t(), key(), RunState.t(), keyword()) ::
          {:ok, t()} | {:error, {:run_plan_capacity_exhausted, map()}}
  def reserve(%__MODULE__{} = capacity, key, %RunState{} = run, opts \\ []) do
    bytes = allocation_bytes(run)
    token = Keyword.get(opts, :memory_capacity_token)
    transferred_retained_bytes = Keyword.get(opts, :transferred_retained_bytes, bytes)
    preserve_working? = Keyword.get(opts, :preserve_working_memory, false)

    cond do
      Map.has_key?(capacity.allocations, key) ->
        {:ok, capacity}

      capacity.allocated_bytes + bytes <= capacity.max_bytes ->
        reserve_node_memory(
          capacity,
          key,
          bytes,
          token,
          transferred_retained_bytes,
          preserve_working?
        )

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

  @spec resize(t(), key(), non_neg_integer()) ::
          {:ok, t()}
          | {:error, {:run_plan_exceeds_node_capacity, non_neg_integer(), pos_integer()}}
          | {:error, {:run_plan_capacity_exhausted, map()}}
  def resize(%__MODULE__{} = capacity, key, required_bytes)
      when is_integer(required_bytes) and required_bytes >= 0 do
    current_bytes = Map.get(capacity.allocations, key, 0)
    next_allocated_bytes = capacity.allocated_bytes - current_bytes + required_bytes

    cond do
      required_bytes > capacity.max_bytes ->
        {:error, {:run_plan_exceeds_node_capacity, required_bytes, capacity.max_bytes}}

      next_allocated_bytes <= capacity.max_bytes ->
        resize_node_memory(capacity, key, required_bytes, next_allocated_bytes)

      true ->
        {:error,
         {:run_plan_capacity_exhausted,
          %{
            required_bytes: required_bytes,
            allocated_bytes: capacity.allocated_bytes,
            max_bytes: capacity.max_bytes
          }}}
    end
  end

  @spec release(t(), key()) :: t()
  def release(%__MODULE__{} = capacity, key) do
    case Map.get(capacity.memory_tokens, key) do
      %MemoryCapacity{} = token -> MemoryCapacity.release(token, server: capacity.memory_server)
      nil -> :ok
    end

    case Map.pop(capacity.allocations, key) do
      {nil, allocations} ->
        %{
          capacity
          | allocations: allocations,
            memory_tokens: Map.delete(capacity.memory_tokens, key)
        }

      {bytes, allocations} ->
        %{
          capacity
          | allocated_bytes: max(capacity.allocated_bytes - bytes, 0),
            allocations: allocations,
            memory_tokens: Map.delete(capacity.memory_tokens, key)
        }
    end
  end

  @spec allocation_bytes(RunState.t()) :: non_neg_integer()
  def allocation_bytes(%RunState{plan: nil}), do: 0

  def allocation_bytes(%RunState{plan: plan}) do
    Budget.retained_term_bytes(plan)
  end

  @spec retained_term_bytes(term()) :: non_neg_integer()
  def retained_term_bytes(term), do: Budget.retained_term_bytes(term)

  @spec diagnostics(t()) :: map()
  def diagnostics(%__MODULE__{} = capacity) do
    %{
      max_bytes: capacity.max_bytes,
      allocated_bytes: capacity.allocated_bytes,
      available_bytes: capacity.max_bytes - capacity.allocated_bytes,
      active_run_count: map_size(capacity.allocations)
    }
  end

  @spec memory_token(t(), key()) :: MemoryCapacity.t() | nil
  def memory_token(%__MODULE__{} = capacity, key), do: Map.get(capacity.memory_tokens, key)

  defp validate_max_bytes!(value) when is_integer(value) and value > 0, do: value

  defp validate_max_bytes!(value) do
    raise ArgumentError,
          ":max_active_run_plan_bytes must be a positive integer, got: #{inspect(value)}"
  end

  defp reserve_node_memory(
         %{coordinate_memory?: false} = capacity,
         key,
         bytes,
         _token,
         _transferred_bytes,
         _preserve_working?
       ) do
    {:ok, put_allocation(capacity, key, bytes, nil)}
  end

  defp reserve_node_memory(
         capacity,
         key,
         bytes,
         %MemoryCapacity{} = token,
         transferred_bytes,
         preserve_working?
       ) do
    reservation = max(bytes, transferred_bytes || bytes)

    result =
      if preserve_working? do
        MemoryCapacity.retain(token, reservation, server: capacity.memory_server)
      else
        MemoryCapacity.transfer(token, reservation, 0, server: capacity.memory_server)
      end

    case result do
      :ok ->
        {:ok, put_allocation(capacity, key, bytes, token)}

      {:error, %MemoryError{} = error} ->
        {:error, shared_capacity_error(capacity, reservation, error)}
    end
  end

  defp reserve_node_memory(capacity, key, bytes, nil, _transferred_bytes, _preserve_working?) do
    case MemoryCapacity.acquire(bytes,
           kind: :run_plan,
           owner: self(),
           server: capacity.memory_server
         ) do
      {:ok, token} ->
        case MemoryCapacity.transfer(token, bytes, 0, server: capacity.memory_server) do
          :ok ->
            {:ok, put_allocation(capacity, key, bytes, token)}

          {:error, %MemoryError{} = error} ->
            MemoryCapacity.release(token, server: capacity.memory_server)
            {:error, shared_capacity_error(capacity, bytes, error)}
        end

      {:error, %MemoryError{} = error} ->
        {:error, shared_capacity_error(capacity, bytes, error)}
    end
  end

  defp resize_node_memory(%{coordinate_memory?: false} = capacity, key, bytes, allocated) do
    {:ok, put_resized_allocation(capacity, key, bytes, allocated)}
  end

  defp resize_node_memory(capacity, key, bytes, allocated) do
    case Map.get(capacity.memory_tokens, key) do
      %MemoryCapacity{} = token ->
        case MemoryCapacity.transfer(token, bytes, 0, server: capacity.memory_server) do
          :ok ->
            {:ok, put_resized_allocation(capacity, key, bytes, allocated)}

          {:error, %MemoryError{} = error} ->
            {:error, shared_capacity_error(capacity, bytes, error)}
        end

      nil ->
        case reserve_node_memory(capacity, key, bytes, nil, bytes, false) do
          {:ok, reserved} -> {:ok, %{reserved | allocated_bytes: allocated}}
          {:error, _reason} = error -> error
        end
    end
  end

  defp put_allocation(capacity, key, bytes, token) do
    %{
      capacity
      | allocated_bytes: capacity.allocated_bytes + bytes,
        allocations: Map.put(capacity.allocations, key, bytes),
        memory_tokens:
          if(token,
            do: Map.put(capacity.memory_tokens, key, token),
            else: capacity.memory_tokens
          )
    }
  end

  defp put_resized_allocation(capacity, key, bytes, allocated) do
    %{
      capacity
      | allocated_bytes: allocated,
        allocations: Map.put(capacity.allocations, key, bytes)
    }
  end

  defp shared_capacity_error(capacity, bytes, error) do
    {:run_plan_capacity_exhausted,
     %{
       required_bytes: bytes,
       allocated_bytes: capacity.allocated_bytes,
       max_bytes: capacity.max_bytes,
       memory_capacity: error.code
     }}
  end
end
