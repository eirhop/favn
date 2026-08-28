defmodule FavnOrchestrator.MemoryCapacity do
  @moduledoc """
  Node-local byte admission for manifest work and retained run plans.

  A lease is bound to a monitored owner. Capacity is released explicitly or
  automatically when that owner exits. Callers reuse and resize one token for
  nested work so the same bytes are not reserved twice.
  """

  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Error
  alias FavnOrchestrator.MemoryCapacity.Ledger

  @enforce_keys [:id]
  defstruct [:id]

  @type t :: %__MODULE__{id: reference()}

  @doc "Acquires a working-byte lease for the calling process or explicit owner."
  @spec acquire(non_neg_integer(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def acquire(bytes, opts \\ []) when is_integer(bytes) and bytes >= 0 and is_list(opts) do
    owner = Keyword.get(opts, :owner, self())
    server = Keyword.get(opts, :server, Coordinator)

    case safe_call(server, {:acquire, owner, bytes, opts}) do
      {:ok, id} -> {:ok, %__MODULE__{id: id}}
      {:error, %Error{} = error} -> {:error, error}
      _unavailable -> {:error, unknown_error(bytes)}
    end
  end

  @doc "Resizes the working portion of an existing lease."
  @spec resize(t(), non_neg_integer(), keyword()) :: :ok | {:error, Error.t()}
  def resize(%__MODULE__{id: id}, bytes, opts \\ [])
      when is_integer(bytes) and bytes >= 0 and is_list(opts) do
    call_result(Keyword.get(opts, :server, Coordinator), {:resize, id, bytes}, bytes)
  end

  @doc "Grows the working portion of a lease without ever shrinking it."
  @spec grow(t(), non_neg_integer(), keyword()) :: :ok | {:error, Error.t()}
  def grow(%__MODULE__{id: id}, bytes, opts \\ [])
      when is_integer(bytes) and bytes >= 0 and is_list(opts) do
    call_result(Keyword.get(opts, :server, Coordinator), {:grow, id, bytes}, bytes)
  end

  @doc "Sets the retained-term portion of an existing lease."
  @spec retain(t(), non_neg_integer(), keyword()) :: :ok | {:error, Error.t()}
  def retain(%__MODULE__{id: id}, bytes, opts \\ [])
      when is_integer(bytes) and bytes >= 0 and is_list(opts) do
    call_result(Keyword.get(opts, :server, Coordinator), {:retain, id, bytes}, bytes)
  end

  @doc "Atomically replaces both retained and working portions of a lease."
  @spec transfer(t(), non_neg_integer(), non_neg_integer(), keyword()) ::
          :ok | {:error, Error.t()}
  def transfer(%__MODULE__{id: id}, retained_bytes, working_bytes \\ 0, opts \\ [])
      when is_integer(retained_bytes) and retained_bytes >= 0 and is_integer(working_bytes) and
             working_bytes >= 0 and is_list(opts) do
    required = retained_bytes + working_bytes

    call_result(
      Keyword.get(opts, :server, Coordinator),
      {:transfer, id, retained_bytes, working_bytes},
      required
    )
  end

  @doc "Atomically transfers lease ownership to another living process."
  @spec handoff(t(), pid(), keyword()) :: :ok | {:error, Error.t()}
  def handoff(%__MODULE__{id: id}, owner, opts \\ []) when is_pid(owner) and is_list(opts) do
    call_result(Keyword.get(opts, :server, Coordinator), {:handoff, id, owner}, 0)
  end

  @doc "Idempotently releases a lease."
  @spec release(t(), keyword()) :: :ok
  def release(%__MODULE__{id: id}, opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, Coordinator)
    ledger = Keyword.get(opts, :ledger, Ledger)

    case safe_call(server, {:release, id}) do
      :ok -> :ok
      _coordinator_unavailable -> release_during_restart(server, ledger, id, 3)
    end
  end

  defp release_during_restart(server, ledger, id, 0) do
    spawn(fn -> retry_release_until_available(server, ledger, id) end)
    :ok
  end

  defp release_during_restart(server, ledger, id, attempts) do
    case safe_call(ledger, {:release, id}) do
      :ok ->
        :ok

      _ledger_not_owner ->
        case safe_call(server, {:release, id}) do
          :ok -> :ok
          _unavailable -> release_during_restart(server, ledger, id, attempts - 1)
        end
    end
  end

  defp retry_release_until_available(server, ledger, id) do
    receive do
    after
      50 ->
        case safe_call(ledger, {:release, id}) do
          :ok ->
            :ok

          _ledger_not_owner ->
            case safe_call(server, {:release, id}) do
              :ok -> :ok
              _unavailable -> retry_release_until_available(server, ledger, id)
            end
        end
    end
  end

  @doc "Acquires a temporary lease, executes the callback, and always releases it."
  @spec with_lease(non_neg_integer(), keyword(), (t() -> result)) ::
          result | {:error, Error.t()}
        when result: term()
  def with_lease(bytes, opts \\ [], fun)
      when is_integer(bytes) and bytes >= 0 and is_list(opts) and is_function(fun, 1) do
    case acquire(bytes, opts) do
      {:ok, token} ->
        try do
          fun.(token)
        after
          release(token, opts)
        end

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @doc "Returns current measured and reserved byte diagnostics."
  @spec diagnostics(keyword()) :: map()
  def diagnostics(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, Coordinator)

    case safe_call(server, :diagnostics) do
      result when is_map(result) -> result
      _unavailable -> %{status: :closed, reason: :memory_capacity_unknown}
    end
  end

  defp call_result(server, message, bytes) do
    case safe_call(server, message) do
      :ok -> :ok
      {:error, %Error{} = error} -> {:error, error}
      _unavailable -> {:error, unknown_error(bytes)}
    end
  end

  defp safe_call(server, message) do
    GenServer.call(server, message, 5_000)
  catch
    :exit, _reason -> {:error, :coordinator_unavailable}
  end

  defp unknown_error(bytes),
    do: %Error{code: :memory_capacity_unknown, required_bytes: bytes}
end
