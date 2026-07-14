defmodule FavnOrchestrator.Storage.Adapter.Memory.RunEvents do
  @moduledoc """
  Run-event append and cursor-query semantics for the in-memory adapter.

  Query option validation mirrors the database adapters so malformed limits or
  cursors cannot silently turn into unbounded reads.
  """

  alias FavnOrchestrator.RunEvents.EventType
  alias FavnOrchestrator.Storage.WriteSemantics

  @max_event_type_filters 32

  @type append_result ::
          {:ok, :ok | :idempotent, [map()], non_neg_integer()} | {:error, term()}

  @doc false
  @spec append([map()], map(), non_neg_integer()) :: append_result()
  def append(current_events, event, current_global_sequence)
      when is_list(current_events) and is_integer(current_global_sequence) do
    existing = Enum.find(current_events, &(Map.get(&1, :sequence) == Map.get(event, :sequence)))

    case WriteSemantics.decide_run_event_append(existing, event) do
      :insert ->
        next_global_sequence = current_global_sequence + 1
        event = Map.put(event, :global_sequence, next_global_sequence)
        next_events = Enum.sort_by([event | current_events], &Map.get(&1, :sequence, 0))
        {:ok, :ok, next_events, next_global_sequence}

      :idempotent ->
        {:ok, :idempotent, current_events, current_global_sequence}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @spec list([map()], keyword()) :: {:ok, [map()]} | {:error, :invalid_opts}
  def list(events, opts) do
    with :ok <- validate_query_opts(opts, :after_sequence) do
      events =
        case Keyword.get(opts, :after_sequence) do
          nil -> events
          sequence -> Enum.filter(events, &(Map.get(&1, :sequence) > sequence))
        end

      {:ok, events |> order(opts) |> limit(opts)}
    end
  end

  @doc false
  @spec list_group([map()], keyword()) :: {:ok, [map()]} | {:error, :invalid_opts}
  def list_group(events, opts) do
    with :ok <- validate_query_opts(opts, :after_global_sequence),
         :ok <- validate_per_run_limit(opts),
         :ok <- validate_latest_per_step(opts),
         {:ok, event_types} <- normalize_event_types(opts) do
      events =
        events
        |> filter_event_types(event_types)
        |> then(fn events ->
          case Keyword.get(opts, :after_global_sequence) do
            nil -> events
            sequence -> Enum.filter(events, &(Map.get(&1, :global_sequence, 0) > sequence))
          end
        end)

      {:ok,
       events
       |> latest_per_step(opts)
       |> per_run_limit(opts)
       |> Enum.sort_by(&sort_key/1)
       |> order(opts)
       |> limit(opts)}
    end
  end

  @doc false
  @spec list_global(%{optional(String.t()) => [map()]}, keyword()) ::
          {:ok, [map()]} | {:error, :cursor_invalid}
  def list_global(events_by_run, opts) do
    after_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit, 200)

    events =
      events_by_run
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(&(is_integer(Map.get(&1, :global_sequence)) and &1.global_sequence > 0))
      |> Enum.sort_by(&Map.fetch!(&1, :global_sequence))

    cond do
      not (is_integer(limit) and limit > 0) ->
        {:error, :cursor_invalid}

      is_nil(after_sequence) ->
        {:ok, events |> Enum.reverse() |> Enum.take(limit) |> Enum.reverse()}

      after_sequence == 0 ->
        {:ok, Enum.take(events, limit)}

      is_integer(after_sequence) and after_sequence > 0 ->
        list_after_global_cursor(events, after_sequence, limit)

      true ->
        {:error, :cursor_invalid}
    end
  end

  defp list_after_global_cursor(events, after_sequence, limit) do
    if Enum.any?(events, &(&1.global_sequence == after_sequence)) do
      {:ok,
       events |> Enum.drop_while(&(&1.global_sequence <= after_sequence)) |> Enum.take(limit)}
    else
      {:error, :cursor_invalid}
    end
  end

  defp validate_query_opts(opts, cursor_key) do
    cursor = Keyword.get(opts, cursor_key)
    limit = Keyword.get(opts, :limit)
    order = Keyword.get(opts, :order, :asc)

    cond do
      not is_nil(cursor) and (not is_integer(cursor) or cursor < 0) -> {:error, :invalid_opts}
      not is_nil(limit) and (not is_integer(limit) or limit <= 0) -> {:error, :invalid_opts}
      order not in [:asc, :desc] -> {:error, :invalid_opts}
      true -> :ok
    end
  end

  defp validate_per_run_limit(opts) do
    case Keyword.get(opts, :per_run_limit) do
      nil -> :ok
      limit when is_integer(limit) and limit > 0 -> :ok
      _invalid -> {:error, :invalid_opts}
    end
  end

  defp validate_latest_per_step(opts) do
    case {Keyword.get(opts, :latest_per_step, false), Keyword.get(opts, :per_run_limit)} do
      {value, nil} when is_boolean(value) -> :ok
      {false, _per_run_limit} -> :ok
      _invalid -> {:error, :invalid_opts}
    end
  end

  defp normalize_event_types(opts) do
    case Keyword.get(opts, :event_types) do
      nil ->
        {:ok, nil}

      event_types
      when is_list(event_types) and event_types != [] and
             length(event_types) <= @max_event_type_filters ->
        if Enum.all?(event_types, &EventType.line_safe?/1),
          do: {:ok, MapSet.new(event_types, &event_type_name/1)},
          else: {:error, :invalid_opts}

      _invalid ->
        {:error, :invalid_opts}
    end
  end

  defp filter_event_types(events, nil), do: events

  defp filter_event_types(events, event_types) do
    Enum.filter(events, &MapSet.member?(event_types, event_type_name(Map.get(&1, :event_type))))
  end

  defp event_type_name(value) when is_atom(value), do: Atom.to_string(value)
  defp event_type_name(value) when is_binary(value), do: value
  defp event_type_name(_value), do: ""

  defp latest_per_step(events, opts) do
    if Keyword.get(opts, :latest_per_step, false) do
      events
      |> Enum.reduce(%{}, fn event, latest ->
        Map.update(latest, step_identity(event), event, fn current ->
          if sort_key(event) > sort_key(current), do: event, else: current
        end)
      end)
      |> Map.values()
    else
      events
    end
  end

  defp step_identity(event) do
    data = Map.get(event, :data, %{})
    asset_step_id = Map.get(data, :asset_step_id) || Map.get(data, "asset_step_id")

    if is_binary(asset_step_id) and asset_step_id != "" do
      {Map.get(event, :run_id), asset_step_id}
    else
      {:event, Map.get(event, :run_id), Map.get(event, :sequence)}
    end
  end

  defp per_run_limit(events, opts) do
    case Keyword.get(opts, :per_run_limit) do
      nil ->
        events

      limit ->
        events
        |> Enum.group_by(&Map.get(&1, :run_id))
        |> Enum.flat_map(fn {_run_id, run_events} ->
          run_events
          |> Enum.sort_by(&Map.get(&1, :sequence, 0), :desc)
          |> Enum.take(limit)
        end)
    end
  end

  defp order(events, opts) do
    case Keyword.get(opts, :order, :asc) do
      :desc -> Enum.reverse(events)
      :asc -> events
    end
  end

  defp limit(events, opts) do
    case Keyword.get(opts, :limit) do
      nil -> events
      limit -> Enum.take(events, limit)
    end
  end

  defp sort_key(event) do
    {Map.get(event, :global_sequence) || 0, Map.get(event, :run_id) || "",
     Map.get(event, :sequence) || 0}
  end
end
