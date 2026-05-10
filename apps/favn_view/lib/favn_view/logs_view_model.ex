defmodule FavnView.LogsViewModel do
  @moduledoc false

  alias Favn.Log.Entry

  @levels Entry.levels()
  @sources Entry.sources()

  def levels, do: @levels
  def sources, do: @sources

  def entry(entry) do
    %{
      id: entry_id(entry),
      global_sequence: Map.get(entry, :global_sequence),
      occurred_at: Map.get(entry, :occurred_at),
      timestamp: time_label(Map.get(entry, :occurred_at)),
      level: normalize_atom(Map.get(entry, :level, :info)),
      level_label: Map.get(entry, :level, :info) |> normalize_atom() |> String.upcase(),
      source: normalize_atom(Map.get(entry, :source, :user_code)),
      source_label: source_label(Map.get(entry, :source, :user_code)),
      run_id: Map.get(entry, :run_id),
      asset_step_id: Map.get(entry, :asset_step_id),
      message: Map.get(entry, :message, "") || "",
      metadata: Map.get(entry, :metadata, %{}) || %{},
      metadata_text: metadata_text(Map.get(entry, :metadata, %{}) || %{}),
      truncated?: Map.get(entry, :truncated, false) == true
    }
  end

  def entries(entries) when is_list(entries), do: Enum.map(entries, &entry/1)

  def filter_entries(entries, search_query, selected_level, selected_source) do
    query = search_query |> to_string() |> String.trim() |> String.downcase()
    selected_level = normalize_filter_value(selected_level)
    selected_source = normalize_filter_value(selected_source)

    Enum.filter(entries, fn entry ->
      level_match?(entry, selected_level) and source_match?(entry, selected_source) and
        search_match?(entry, query)
    end)
  end

  def plain_text(entries) when is_list(entries) do
    entries
    |> Enum.map(fn entry ->
      [entry.timestamp, entry.level_label, entry.source_label, entry.message]
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.join("  ")
    end)
    |> Enum.join("\n\n")
  end

  def latest_cursor(entries, scope, filter) do
    entries
    |> Enum.map(&Map.get(&1, :global_sequence))
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> nil end)
    |> case do
      nil ->
        nil

      sequence ->
        %Favn.Log.Cursor{
          scope: scope,
          run_id: filter.run_id,
          asset_step_id: filter.asset_step_id,
          global_sequence: sequence
        }
    end
  end

  def merge_entries(existing, incoming) do
    (existing ++ List.wrap(incoming))
    |> Enum.uniq_by(&dedupe_key/1)
    |> Enum.sort_by(&{Map.get(&1, :global_sequence) || 0, entry_id(&1)})
  end

  def trim_latest(entries, limit) do
    entries
    |> Enum.sort_by(&{Map.get(&1, :global_sequence) || 0, entry_id(&1)})
    |> Enum.take(-limit)
  end

  def short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  def short_id(id) when is_binary(id), do: id
  def short_id(_id), do: "unknown"

  def status_label(status) when status in [:ok, "ok"], do: "Succeeded"
  def status_label(status) when status in [:running, "running"], do: "Running"
  def status_label(status) when status in [:retrying, "retrying"], do: "Retrying"
  def status_label(status) when status in [:pending, "pending"], do: "Pending"
  def status_label(status) when status in [:partial, "partial"], do: "Partial"
  def status_label(status) when status in [:error, "error"], do: "Failed"
  def status_label(status) when status in [:blocked, "blocked"], do: "Blocked"
  def status_label(status) when status in [:cancelled, "cancelled"], do: "Cancelled"
  def status_label(status) when status in [:skipped_fresh, "skipped_fresh"], do: "Skipped fresh"
  def status_label(status) when status in [:timed_out, "timed_out"], do: "Timed out"
  def status_label(nil), do: "Unknown"
  def status_label(status), do: humanize(status)

  def status_tone(status) when status in [:ok, "ok"], do: :success

  def status_tone(status)
      when status in [:running, :pending, :retrying, "running", "pending", "retrying"], do: :info

  def status_tone(status) when status in [:partial, "partial"], do: :warning

  def status_tone(status)
      when status in [:error, :timed_out, :blocked, "error", "timed_out", "blocked"], do: :error

  def status_tone(_status), do: :neutral

  def timestamp_label(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")

  def timestamp_label(_value), do: "-"

  def duration_label(%DateTime{} = started_at, %DateTime{} = finished_at) do
    DateTime.diff(finished_at, started_at, :millisecond) |> duration_ms_label()
  end

  def duration_label(%DateTime{} = started_at, nil) do
    DateTime.diff(DateTime.utc_now(), started_at, :millisecond) |> duration_ms_label()
  end

  def duration_label(_started_at, _finished_at), do: "-"
  def duration_ms_label(value) when is_integer(value) and value < 1_000, do: "#{value} ms"
  def duration_ms_label(value) when is_integer(value), do: "#{Float.round(value / 1_000, 1)} s"
  def duration_ms_label(_value), do: "-"

  def display_name(asset_ref) when is_binary(asset_ref),
    do: asset_ref |> String.split(".") |> List.last()

  def display_name(_asset_ref), do: nil

  def ref_label({module, name}), do: "#{inspect(module)}.#{name}"
  def ref_label(%{"module" => module, "name" => name}), do: "#{module}.#{name}"
  def ref_label(ref) when is_atom(ref), do: Atom.to_string(ref)
  def ref_label(ref) when is_binary(ref), do: ref
  def ref_label(nil), do: nil
  def ref_label(ref), do: inspect(ref)

  def deterministic_step_id(run_id, asset_ref), do: safe_id("#{run_id}:#{asset_ref}")
  def safe_id(value), do: value |> to_string() |> String.replace(~r/[^a-zA-Z0-9_-]+/, "-")

  defp entry_id(entry),
    do:
      Map.get(entry, :id) || "log-#{Map.get(entry, :global_sequence) || System.unique_integer()}"

  defp dedupe_key(entry) do
    Map.get(entry, :global_sequence) || Map.get(entry, :id) ||
      {Map.get(entry, :producer_id), Map.get(entry, :producer_sequence)}
  end

  defp time_label(%DateTime{} = value), do: Calendar.strftime(value, "%H:%M:%S")
  defp time_label(_value), do: "--:--:--"

  defp source_label(source), do: source |> normalize_atom() |> String.replace("_", ":")
  defp normalize_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_atom(value), do: to_string(value)

  defp normalize_filter_value(value) when value in [nil, "", "all", :all], do: "all"
  defp normalize_filter_value(value), do: normalize_atom(value)

  defp level_match?(_entry, "all"), do: true
  defp level_match?(entry, level), do: entry.level == level

  defp source_match?(_entry, "all"), do: true
  defp source_match?(entry, source), do: entry.source == source

  defp search_match?(_entry, ""), do: true

  defp search_match?(entry, query) do
    [entry.message, entry.source_label, entry.level_label, entry.metadata_text]
    |> Enum.join("\n")
    |> String.downcase()
    |> String.contains?(query)
  end

  defp metadata_text(metadata) when map_size(metadata) == 0, do: ""
  defp metadata_text(metadata), do: inspect(metadata, pretty: true, limit: 50)

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
