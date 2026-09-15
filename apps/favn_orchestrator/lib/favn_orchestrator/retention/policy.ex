defmodule FavnOrchestrator.Retention.Policy do
  @moduledoc """
  Platform retention configuration. Durations are seconds since durable settlement.

  Optional history cleanup defaults to disabled. Receipt expiry is always enforced;
  the worker removes expired receipts even with optional history disabled. Workspace
  holds postpone physical deletion, including receipts, without extending replay.
  """

  @families ~w(receipts logs execution_history operations registry sessions idempotency maintenance)a
  @optional @families -- [:receipts]
  @week 604_800
  defstruct enabled?: false,
            periods: Map.new(@optional, &{&1, :retain_forever}),
            excluded_workspace_ids: [],
            interval_ms: 60_000,
            row_limit: 250,
            scan_limit: 1_000,
            turn_budget_ms: 5_000

  @type family ::
          :receipts
          | :logs
          | :execution_history
          | :operations
          | :registry
          | :sessions
          | :idempotency
          | :maintenance
  @type t :: %__MODULE__{
          enabled?: boolean(),
          periods: %{family() => pos_integer() | :retain_forever},
          excluded_workspace_ids: [String.t()],
          interval_ms: pos_integer(),
          row_limit: pos_integer(),
          scan_limit: pos_integer(),
          turn_budget_ms: pos_integer()
        }

  @doc "The fixed family rotation; this is not a handler registry."
  @spec families() :: [family()]
  def families, do: @families

  @doc "Validates configuration once at the boundary; unknown fields fail closed."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, :invalid_retention_policy}
  def new(options) when is_list(options), do: new(Map.new(options))
  def new(%__MODULE__{} = policy), do: validate(policy)

  def new(options) when is_map(options) do
    if Map.keys(options) -- Map.keys(Map.from_struct(%__MODULE__{})) == [] do
      validate(struct!(__MODULE__, options))
    else
      {:error, :invalid_retention_policy}
    end
  end

  def new(_), do: {:error, :invalid_retention_policy}

  defp validate(p) do
    if is_boolean(p.enabled?) and is_map(p.periods) and
         Map.keys(p.periods) -- @optional == [] and
         Enum.all?(p.periods, fn {_, v} -> v == :retain_forever or (is_integer(v) and v > 0) end) and
         is_list(p.excluded_workspace_ids) and length(p.excluded_workspace_ids) <= 1_000 and
         Enum.all?(p.excluded_workspace_ids, &(is_binary(&1) and byte_size(&1) in 1..255)) and
         bounded?(p.interval_ms, 1_000, 86_400_000) and bounded?(p.row_limit, 5, 1_000) and
         bounded?(p.scan_limit, p.row_limit, 10_000) and bounded?(p.turn_budget_ms, 100, 5_000) do
      {:ok,
       %{
         p
         | periods: Map.merge(%__MODULE__{}.periods, p.periods),
           excluded_workspace_ids: Enum.sort(Enum.uniq(p.excluded_workspace_ids))
       }}
    else
      {:error, :invalid_retention_policy}
    end
  end

  @doc "Returns the eligible age, or `:retain_forever` for disabled families."
  @spec period(t(), family()) :: pos_integer() | :retain_forever
  def period(_, :receipts), do: @week + 300
  def period(%{enabled?: false}, _), do: :retain_forever

  def period(p, family) do
    case Map.fetch!(p.periods, family) do
      :retain_forever ->
        :retain_forever

      seconds when family in [:logs, :execution_history, :operations, :registry] ->
        max(seconds, @week + 300)

      seconds ->
        seconds
    end
  end

  @doc "Accepted command age in seconds; deletion must also allow clock skew."
  @spec command_window_seconds() :: pos_integer()
  def command_window_seconds, do: @week

  @doc "Stable JSON representation stored in the singleton maintenance job."
  @spec encode(t()) :: map()
  def encode(p) do
    p
    |> Map.from_struct()
    |> Map.new(fn
      {:periods, periods} ->
        {"periods",
         Map.new(periods, fn {k, v} ->
           {Atom.to_string(k), if(v == :retain_forever, do: "retain_forever", else: v)}
         end)}

      {k, v} ->
        {Atom.to_string(k), v}
    end)
  end

  @doc "Decodes stored configuration without creating atoms from input."
  @spec decode(map()) :: {:ok, t()} | {:error, :invalid_retention_policy}
  def decode(value) when is_map(value) do
    keys = Map.keys(Map.from_struct(%__MODULE__{}))

    with true <- Map.keys(value) -- Enum.map(keys, &Atom.to_string/1) == [],
         periods when is_map(periods) <- value["periods"],
         true <- Map.keys(periods) -- Enum.map(@optional, &Atom.to_string/1) == [] do
      options = Map.new(keys, &{&1, value[Atom.to_string(&1)]})

      periods =
        Map.new(@optional, fn k ->
          v = periods[Atom.to_string(k)]
          {k, if(v == "retain_forever", do: :retain_forever, else: v)}
        end)

      new(Map.put(options, :periods, periods))
    else
      _ -> {:error, :invalid_retention_policy}
    end
  end

  def decode(_), do: {:error, :invalid_retention_policy}

  defp bounded?(v, min, max), do: is_integer(v) and v >= min and v <= max
end
