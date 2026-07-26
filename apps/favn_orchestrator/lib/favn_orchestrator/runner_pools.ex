defmodule FavnOrchestrator.RunnerPools do
  @moduledoc """
  Provider-neutral, boot-frozen policy for logical runner pools.

  Pool names describe interchangeable runner environments. Infrastructure
  details such as images, CPU, memory, and prices intentionally live outside
  Favn.
  """

  @default_idle_grace_ms 15_000
  @max_idle_grace_ms 3_600_000
  @max_pools 64

  @type mode :: :elastic | :resident
  @type policy :: %{
          required(:mode) => mode(),
          required(:idle_grace_ms) => non_neg_integer() | :infinity
        }
  @type t :: %{required(String.t()) => policy()}

  @spec default() :: keyword()
  def default, do: [default: [mode: :elastic, idle_grace_ms: @default_idle_grace_ms]]

  @spec normalize(term()) :: {:ok, t()} | {:error, term()}
  def normalize(value) when is_list(value) and length(value) in 1..@max_pools do
    if Keyword.keyword?(value) do
      Enum.reduce_while(value, {:ok, %{}}, fn {name, options}, {:ok, pools} ->
        with {:ok, runtime_name} <- Favn.RunnerPool.encode(name),
             {:ok, policy} <- normalize_policy(options) do
          {:cont, {:ok, Map.put(pools, runtime_name, policy)}}
        else
          {:error, reason} -> {:halt, {:error, {:invalid_runner_pool_policy, name, reason}}}
        end
      end)
    else
      {:error, {:invalid_runner_pools, value}}
    end
  end

  def normalize(value) when is_map(value) and map_size(value) in 1..@max_pools do
    Enum.reduce_while(value, {:ok, %{}}, fn {name, options}, {:ok, pools} ->
      with :ok <- Favn.RunnerPool.validate_runtime(name),
           {:ok, policy} <- normalize_policy(options) do
        {:cont, {:ok, Map.put(pools, name, policy)}}
      else
        {:error, reason} -> {:halt, {:error, {:invalid_runner_pool_policy, name, reason}}}
      end
    end)
  end

  def normalize(value), do: {:error, {:invalid_runner_pools, value}}

  @spec normalize!(term()) :: t()
  def normalize!(value) do
    case normalize(value) do
      {:ok, pools} -> pools
      {:error, reason} -> raise ArgumentError, "invalid runner pools: #{inspect(reason)}"
    end
  end

  @spec fetch(t(), atom() | String.t()) :: {:ok, policy()} | {:error, term()}
  def fetch(pools, name) when is_map(pools) do
    with {:ok, runtime_name} <- runtime_name(name),
         {:ok, policy} <- Map.fetch(pools, runtime_name) do
      {:ok, policy}
    else
      :error -> {:error, {:runner_pool_not_configured, name}}
      {:error, _reason} = error -> error
    end
  end

  defp runtime_name(name) when is_atom(name), do: Favn.RunnerPool.encode(name)

  defp runtime_name(name) when is_binary(name) do
    case Favn.RunnerPool.validate_runtime(name) do
      :ok -> {:ok, name}
      {:error, _reason} = error -> error
    end
  end

  defp runtime_name(name), do: {:error, {:invalid_runner_pool, name}}

  defp normalize_policy(options) when is_map(options) do
    allowed = MapSet.new(["mode", "idle_grace_ms", :mode, :idle_grace_ms])

    if Map.keys(options) |> Enum.all?(&MapSet.member?(allowed, &1)) do
      mode = Map.get(options, :mode, Map.get(options, "mode", "elastic"))

      idle_grace_ms =
        cond do
          Map.has_key?(options, :idle_grace_ms) -> Map.fetch!(options, :idle_grace_ms)
          Map.has_key?(options, "idle_grace_ms") -> Map.fetch!(options, "idle_grace_ms")
          true -> :absent
        end

      normalize_policy(
        mode: normalize_mode(mode),
        idle_grace_ms: idle_grace_ms
      )
    else
      {:error, {:invalid_policy_options, options}}
    end
  end

  defp normalize_policy(options) when is_list(options) do
    if Keyword.keyword?(options) and
         Enum.all?(Keyword.keys(options), &(&1 in [:mode, :idle_grace_ms])) do
      mode = normalize_mode(Keyword.get(options, :mode, :elastic))

      case mode do
        :elastic ->
          grace =
            case Keyword.get(options, :idle_grace_ms, @default_idle_grace_ms) do
              :absent -> @default_idle_grace_ms
              value -> value
            end

          if is_integer(grace) and grace >= 0 and grace <= @max_idle_grace_ms,
            do: {:ok, %{mode: :elastic, idle_grace_ms: grace}},
            else: {:error, {:invalid_idle_grace_ms, grace}}

        :resident ->
          if Keyword.get(options, :idle_grace_ms, :absent) != :absent,
            do: {:error, :resident_idle_grace_not_allowed},
            else: {:ok, %{mode: :resident, idle_grace_ms: :infinity}}

        other ->
          {:error, {:invalid_mode, other}}
      end
    else
      {:error, {:invalid_policy_options, options}}
    end
  end

  defp normalize_policy(value), do: {:error, {:invalid_policy_options, value}}

  defp normalize_mode("elastic"), do: :elastic
  defp normalize_mode("resident"), do: :resident
  defp normalize_mode(value), do: value
end
