defmodule FavnOrchestrator.LogWriter do
  @moduledoc """
  Persists trusted backend logs and broadcasts them only after durable write.
  """

  alias FavnOrchestrator.Logs
  alias Favn.Log.Entry, as: PublicLogEntry
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.LogEntry
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @doc "Persists one bounded workspace log batch and broadcasts committed entries."
  @spec write(WorkspaceContext.t(), term() | [term()], keyword()) ::
          {:ok, [FavnOrchestrator.Persistence.Results.LogEntry.t()]} | {:error, term()}
  def write(%WorkspaceContext{} = context, entries, opts \\ []) when is_list(opts) do
    entries = if is_list(entries), do: entries, else: [entries]

    with :ok <- validate_opts(opts),
         {:ok, normalized} <- normalize_entries(entries),
         {:ok, batch_id} <- batch_id(normalized, opts) do
      command = %AppendLogBatch{
        workspace_context: context,
        command_id: Keyword.get(opts, :command_id) || "logs:" <> digest(batch_id),
        batch_id: batch_id,
        entries: normalized,
        occurred_at: Keyword.get(opts, :occurred_at, DateTime.utc_now())
      }

      with {:ok, persisted_entries} <- Persistence.stores().logs.append_batch(command) do
        Enum.each(persisted_entries, &Logs.broadcast_log_entry/1)
        {:ok, persisted_entries}
      end
    end
  end

  defp normalize_entries(entries) when entries != [] and length(entries) <= 1_000 do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case normalize_entry(entry) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end)
  end

  defp normalize_entries(_entries), do: {:error, :invalid_log_batch}

  defp normalize_entry(entry) when is_map(entry) do
    level = field(entry, :level)
    source = field(entry, :source)
    message = field(entry, :message)
    occurred_at = field(entry, :occurred_at) || DateTime.utc_now()

    if level in [:debug, :info, :warning, :error] and
         known_source?(source) and is_binary(message) and
         match?(%DateTime{}, occurred_at) do
      metadata =
        entry
        |> field(:metadata, %{})
        |> then(fn value -> if is_map(value), do: value, else: %{} end)
        |> Map.merge(
          Map.take(entry, [
            :asset_step_id,
            :node_key,
            :asset_ref,
            :runner_execution_id,
            :attempt,
            :producer_id,
            :producer_sequence,
            :stream,
            :truncated
          ])
        )

      {:ok,
       %LogEntry{
         source: to_string(source),
         level: level,
         message: message,
         occurred_at: occurred_at,
         run_id: field(entry, :run_id),
         metadata: metadata
       }}
    else
      {:error, :invalid_log_entry}
    end
  end

  defp normalize_entry(_entry), do: {:error, :invalid_log_entry}

  defp batch_id(entries, opts) do
    case Keyword.get(opts, :batch_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      nil -> {:ok, "log-batch:" <> digest(:erlang.term_to_binary(entries))}
      _invalid -> {:error, :invalid_log_batch_id}
    end
  end

  defp known_source?(source) when is_atom(source), do: source in PublicLogEntry.sources()

  defp known_source?(source) when is_binary(source),
    do: Enum.any?(PublicLogEntry.sources(), &(Atom.to_string(&1) == source))

  defp known_source?(_source), do: false

  defp validate_opts(opts) do
    if Keyword.keyword?(opts) do
      case Keyword.keys(opts) -- [:batch_id, :command_id, :occurred_at] do
        [] -> :ok
        unknown -> {:error, {:unknown_log_options, unknown}}
      end
    else
      {:error, :invalid_log_options}
    end
  end

  defp digest(value) do
    :crypto.hash(:sha256, value)
    |> Base.encode16(case: :lower)
  end

  defp field(value, key, default \\ nil)
  defp field(%{__struct__: _module} = value, key, default), do: Map.get(value, key, default)
  defp field(value, key, default) when is_map(value), do: Map.get(value, key, default)
end
