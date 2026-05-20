defmodule FavnView.Components.OutputMetadata do
  @moduledoc """
  Renders operator-facing asset output metadata.
  """

  use FavnView, :html

  @priority_keys [
    "rows_written",
    "rows_read",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "relation",
    "table",
    "schema",
    "catalog",
    "partition",
    "partition_month",
    "window",
    "mode",
    "endpoint",
    "source",
    "loaded_at",
    "started_at",
    "finished_at"
  ]

  @priorities @priority_keys |> Enum.with_index() |> Map.new()
  @terminal_failures [
    :error,
    :timed_out,
    :cancelled,
    :blocked,
    "error",
    "timed_out",
    "cancelled",
    "blocked"
  ]
  @active_statuses [:pending, :running, "pending", "running"]

  attr :id, :string, default: "output-metadata"
  attr :metadata, :any, default: nil
  attr :status, :any, default: nil
  attr :title, :string, default: "Output metadata"
  attr :class, :string, default: nil
  attr :initial_rows, :integer, default: 12

  def output_metadata(assigns) do
    rows = metadata_rows(assigns.metadata)
    {visible_rows, hidden_rows} = Enum.split(rows, assigns.initial_rows)

    assigns =
      assigns
      |> assign(:rows, rows)
      |> assign(:visible_rows, visible_rows)
      |> assign(:hidden_rows, hidden_rows)
      |> assign(:raw_json, raw_json(assigns.metadata))
      |> assign(:empty?, empty_metadata?(assigns.metadata))
      |> assign(:failed?, failed_status?(assigns.status))
      |> assign(:active?, active_status?(assigns.status))

    ~H"""
    <section
      id={@id}
      phx-hook="FavnClipboard"
      class={[
        "rounded-box border border-base-content/10 bg-base-content/[0.025] p-4",
        @class
      ]}
      data-testid="output-metadata"
    >
      <div class="flex items-center justify-between gap-3">
        <h3 class="text-sm font-medium">{@title}</h3>
        <button
          :if={!@empty?}
          type="button"
          class="btn btn-ghost btn-xs rounded-field"
          data-copy-text={@raw_json}
          data-testid="output-metadata-copy"
        >
          <.icon name="hero-clipboard-document" class="size-4" /> Copy JSON
        </button>
      </div>

      <p
        :if={@empty? && @failed?}
        class="mt-3 text-sm text-base-content/55"
        data-testid="output-metadata-empty"
      >
        No output metadata available because the attempt failed before completion.
      </p>
      <p
        :if={@empty? && @active?}
        class="mt-3 text-sm text-base-content/55"
        data-testid="output-metadata-empty"
      >
        No output metadata yet.
      </p>
      <p
        :if={@empty? && !@failed? && !@active?}
        class="mt-3 text-sm text-base-content/55"
        data-testid="output-metadata-empty"
      >
        No output metadata returned.
      </p>

      <dl :if={!@empty?} class="mt-4 divide-y divide-base-content/10 text-sm">
        <.metadata_row :for={row <- @visible_rows} row={row} />
      </dl>

      <details :if={@hidden_rows != []} class="mt-3 rounded-box border border-base-content/10 p-3">
        <summary class="cursor-pointer text-xs font-medium text-base-content/65">
          Show {length(@hidden_rows)} more metadata {if(length(@hidden_rows) == 1,
            do: "field",
            else: "fields"
          )}
        </summary>
        <dl class="mt-3 divide-y divide-base-content/10 text-sm">
          <.metadata_row :for={row <- @hidden_rows} row={row} />
        </dl>
      </details>

      <details :if={!@empty?} class="mt-3 rounded-box border border-base-content/10 p-3">
        <summary class="cursor-pointer text-xs font-medium text-base-content/65">Raw JSON</summary>
        <pre class="mt-3 max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-box bg-base-300/35 p-3 text-xs text-base-content/80"><code>{@raw_json}</code></pre>
      </details>
    </section>
    """
  end

  attr :row, :map, required: true

  defp metadata_row(assigns) do
    ~H"""
    <div class="grid gap-1 py-2 first:pt-0 last:pb-0 sm:grid-cols-[11rem_minmax(0,1fr)] sm:gap-4">
      <dt class="text-xs text-base-content/45">{@row.label}</dt>
      <dd class={["break-words font-medium", @row.mono? && "font-mono text-xs"]}>{@row.value}</dd>
    </div>
    """
  end

  defp metadata_rows(metadata) when is_map(metadata) and map_size(metadata) == 0, do: []

  defp metadata_rows(metadata) when is_map(metadata) do
    metadata
    |> flatten_map()
    |> Enum.map(fn {key, value} ->
      %{key: key, label: label(key), value: value_label(value), mono?: structured?(value)}
    end)
    |> Enum.sort_by(&{priority(&1.key), &1.key})
  end

  defp metadata_rows(nil), do: []

  defp metadata_rows(value) do
    [%{key: "result", label: "Result", value: value_label(value), mono?: structured?(value)}]
  end

  defp flatten_map(map), do: flatten_map(map, nil)

  defp flatten_map(map, prefix) do
    Enum.flat_map(map, fn {key, value} ->
      key = joined_key(prefix, key)

      case value do
        nested when is_map(nested) and map_size(nested) > 0 -> flatten_map(nested, key)
        other -> [{key, other}]
      end
    end)
  end

  defp joined_key(nil, key), do: key_string(key)
  defp joined_key(prefix, key), do: prefix <> "." <> key_string(key)

  defp key_string(key) when is_atom(key), do: Atom.to_string(key)
  defp key_string(key) when is_binary(key), do: key
  defp key_string(key), do: inspect(key)

  defp label(key) do
    if String.contains?(key, ".") do
      key
    else
      key
      |> String.replace("_", " ")
      |> String.capitalize()
    end
  end

  defp priority(key) do
    first_segment = key |> String.split(".", parts: 2) |> List.first()
    Map.get(@priorities, key, Map.get(@priorities, first_segment, 999))
  end

  defp value_label(nil), do: "null"
  defp value_label(value) when is_binary(value), do: value
  defp value_label(value) when is_atom(value), do: Atom.to_string(value)
  defp value_label(%DateTime{} = value), do: DateTime.to_string(value)
  defp value_label(%Date{} = value), do: Date.to_iso8601(value)
  defp value_label(%Time{} = value), do: Time.to_iso8601(value)
  defp value_label(value) when is_boolean(value), do: to_string(value)
  defp value_label(value) when is_number(value), do: to_string(value)
  defp value_label(value) when is_map(value) or is_list(value), do: compact_json(value)
  defp value_label(value), do: inspect(value)

  defp structured?(value), do: is_map(value) or is_list(value)

  defp empty_metadata?(nil), do: true
  defp empty_metadata?(metadata) when is_map(metadata), do: map_size(metadata) == 0
  defp empty_metadata?(_metadata), do: false

  defp failed_status?(status), do: status in @terminal_failures

  defp active_status?(status), do: status in @active_statuses

  defp raw_json(value), do: encode_json(value, pretty: true)
  defp compact_json(value), do: encode_json(value, pretty: false)

  defp encode_json(value, opts) do
    case Jason.encode(json_value(value), opts) do
      {:ok, json} ->
        json

      {:error, _reason} ->
        inspect(value, pretty: opts[:pretty], limit: 50, printable_limit: 2_000)
    end
  end

  defp json_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_value(%Date{} = value), do: Date.to_iso8601(value)
  defp json_value(%Time{} = value), do: Time.to_iso8601(value)
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)

  defp json_value(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value), do: value

  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)

  defp json_value(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {key_string(key), json_value(item)} end)
  end

  defp json_value(value), do: inspect(value)
end
