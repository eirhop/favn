defmodule FavnOrchestrator.LogWriter do
  @moduledoc """
  Persists trusted backend logs and broadcasts them only after durable write.
  """

  alias FavnOrchestrator.Logs
  alias FavnOrchestrator.Storage

  @spec write(term() | [term()]) :: {:ok, [term()]} | {:error, term()}
  def write(entries) when is_list(entries) do
    redacted_entries = Enum.map(entries, &redact_entry/1)

    with {:ok, persisted_entries} <- Storage.persist_log_entries(redacted_entries) do
      Enum.each(persisted_entries, &Logs.broadcast_log_entry/1)
      {:ok, persisted_entries}
    end
  end

  def write(entry), do: write([entry])

  defp redact_entry(entry) do
    entry
    |> normalize_entry()
    |> apply_redaction_policy()
  end

  defp normalize_entry(%{__struct__: _struct} = entry), do: entry

  defp normalize_entry(attrs) when is_map(attrs) do
    case Code.ensure_loaded(Favn.Log.Entry) do
      {:module, Favn.Log.Entry} -> Favn.Log.Entry.normalize(attrs)
      _other -> attrs
    end
  end

  defp normalize_entry(entry), do: entry

  defp apply_redaction_policy(entry) do
    with {:module, Favn.Log.Redactor} <- Code.ensure_loaded(Favn.Log.Redactor),
         true <- function_exported?(Favn.Log.Redactor, :redact, 2) do
      case Favn.Log.Redactor.redact(entry, redaction_policy()) do
        {redacted_entry, _redacted?} -> redacted_entry
        redacted_entry -> redacted_entry
      end
    else
      _other -> entry
    end
  end

  defp redaction_policy do
    Application.get_env(:favn_orchestrator, :log_redaction_policy)
  end
end
