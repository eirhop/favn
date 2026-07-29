defmodule FavnView.Components.OutputMetadata do
  @moduledoc """
  Renders operator-facing asset output metadata.
  """

  use FavnView, :html

  @priority_keys [
    "quality_status",
    "write_outcome",
    "reason",
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
  @active_statuses [:pending, :running, :retrying, "pending", "running", "retrying"]

  @doc """
  The one line worth reading about what an attempt wrote.

  Output metadata is a bag of twenty-odd fields, and rendering all of them gives
  every field equal weight. Three facts answer "did the data come out right?":
  how many rows landed, where they landed, and whether the quality checks were
  satisfied. The rest is forensic and belongs behind disclosure.

  Returns `nil` when the metadata says nothing about a write, so a caller can
  omit the line rather than print "unknown".

      iex> alias FavnView.Components.OutputMetadata
      iex> OutputMetadata.outcome(%{"rows_written" => 1204, "relation" => "crm.daily"}, :ok)
      %{headline: "Wrote 1,204 rows", target: "crm.daily", tone: :success, checks: nil}

  A SQL asset reports where it materialised rather than a row count, so the
  headline says what happened and the target says where:

      iex> alias FavnView.Components.OutputMetadata
      iex> metadata = %{
      ...>   "write_outcome" => "written",
      ...>   "materialized" => %{"schema" => "mart", "name" => "pipeline_daily"}
      ...> }
      iex> OutputMetadata.outcome(metadata, :ok)
      %{headline: "Table written", target: "mart.pipeline_daily", tone: :success, checks: nil}

      iex> alias FavnView.Components.OutputMetadata
      iex> OutputMetadata.outcome(%{"write_outcome" => "no_op"}, :ok)
      %{headline: "Kept the existing table", target: nil, tone: :info, checks: nil}

      iex> FavnView.Components.OutputMetadata.outcome(%{}, :ok)
      nil
  """
  @spec outcome(any(), any()) :: map() | nil
  def outcome(metadata, status) when is_map(metadata) and map_size(metadata) > 0 do
    rows = row_count(metadata)
    write_outcome = metadata_value(metadata, :write_outcome)
    checks = metadata |> metadata_value(:check_results) |> normalize_checks()

    case write_headline(rows, write_outcome, status) do
      nil ->
        nil

      {headline, tone} ->
        %{
          headline: headline,
          target: write_target(metadata),
          tone: tone,
          checks: checks_label(checks)
        }
    end
  end

  def outcome(_metadata, _status), do: nil

  attr :id, :string, default: "output-metadata"
  attr :metadata, :any, default: nil
  attr :status, :any, default: nil
  attr :title, :string, default: "Output metadata"
  attr :class, :string, default: nil

  def output_metadata(assigns) do
    assigns =
      assigns
      |> assign(:rows, metadata_rows(assigns.metadata))
      |> assign(:raw_json, raw_json(assigns.metadata))
      |> assign(:empty?, empty_metadata?(assigns.metadata))
      |> assign(:failed?, failed_status?(assigns.status))
      |> assign(:active?, active_status?(assigns.status))
      |> assign(:check_summary, check_summary(assigns.metadata, assigns.status))

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
        <.copy_button
          :if={!@empty?}
          value={@raw_json}
          label="Copy JSON"
          variant={:ghost}
          size={:xs}
          data-testid="output-metadata-copy"
        />
      </div>

      <div
        :if={@check_summary}
        class={[
          "mt-4 rounded-box border p-3",
          check_summary_class(@check_summary.tone)
        ]}
        data-testid="sql-check-summary"
        data-quality-status={@check_summary.quality_status}
        data-write-outcome={@check_summary.write_outcome}
      >
        <div class="flex flex-wrap items-center justify-between gap-2">
          <p class="text-sm font-medium">{@check_summary.title}</p>
          <div class="flex flex-wrap gap-1.5">
            <span class={check_badge_class(@check_summary.tone)}>
              {@check_summary.quality_label}
            </span>
            <span :if={@check_summary.write_label} class="badge badge-outline badge-sm">
              {@check_summary.write_label}
            </span>
          </div>
        </div>
        <p :if={@check_summary.description} class="mt-1 text-xs opacity-70">
          {@check_summary.description}
        </p>

        <div
          :if={@check_summary.tone != :success and @check_summary.checks != []}
          class="mt-3 grid gap-2"
        >
          <article
            :for={check <- @check_summary.checks}
            class="rounded-field border border-current/15 bg-base-100/55 p-2.5"
            data-testid="sql-check-result"
            data-check-outcome={check.outcome}
            data-check-origin={check.origin}
          >
            <div class="flex flex-wrap items-center justify-between gap-2">
              <div class="flex flex-wrap items-center gap-2">
                <span class={check_origin_badge(check.origin)}>{check.origin_label}</span>
                <p class="font-mono text-xs font-semibold">{check.name}</p>
              </div>
              <p :if={check.claim_id} class="mt-1 font-mono text-[0.7rem] opacity-55">
                {check.claim_id}
              </p>
              <span class={check_badge_class(check.tone)}>{check.outcome_label}</span>
            </div>
            <p class="mt-1 text-xs opacity-65">
              {check.phase_label}{check.duration_label}
            </p>
            <p :if={check.message} class="mt-1 text-xs">{check.message}</p>
            <dl :if={check.metrics != []} class="mt-2 grid gap-1 text-xs">
              <div :for={{key, value} <- check.metrics} class="flex justify-between gap-3">
                <dt class="opacity-60">{key}</dt>
                <dd class="break-all font-mono">{value}</dd>
              </div>
            </dl>
          </article>
        </div>
      </div>

      <p
        :if={@empty? && @failed?}
        class="mt-3 text-sm favn-text-muted"
        data-testid="output-metadata-empty"
      >
        No output metadata available because the attempt failed before completion.
      </p>
      <p
        :if={@empty? && @active?}
        class="mt-3 text-sm favn-text-muted"
        data-testid="output-metadata-empty"
      >
        No output metadata yet.
      </p>
      <p
        :if={@empty? && !@failed? && !@active?}
        class="mt-3 text-sm favn-text-muted"
        data-testid="output-metadata-empty"
      >
        No output metadata returned.
      </p>

      <details :if={!@empty?} class="mt-3 rounded-box border border-base-content/10 p-3">
        <summary class="cursor-pointer text-xs font-medium favn-text-muted">
          All {length(@rows)} metadata {if(length(@rows) == 1, do: "field", else: "fields")}
        </summary>
        <dl class="mt-3 divide-y divide-base-content/10 text-sm">
          <.metadata_row :for={row <- @rows} row={row} />
        </dl>
        <pre class="mt-3 max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-box bg-base-300/35 p-3 text-xs favn-text-muted"><code>{@raw_json}</code></pre>
      </details>
    </section>
    """
  end

  attr :row, :map, required: true

  defp metadata_row(assigns) do
    ~H"""
    <div class="grid gap-1 py-2 first:pt-0 last:pb-0 sm:grid-cols-[11rem_minmax(0,1fr)] sm:gap-4">
      <dt class="text-xs favn-text-subtle">{@row.label}</dt>
      <dd class={["break-words font-medium", @row.mono? && "font-mono text-xs"]}>{@row.value}</dd>
    </div>
    """
  end

  defp metadata_rows(metadata) when is_map(metadata) and map_size(metadata) == 0, do: []

  defp metadata_rows(metadata) when is_map(metadata) do
    metadata
    |> Map.drop([:check_results, "check_results"])
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

  defp check_summary(metadata, status) when is_map(metadata) do
    quality_status = metadata_value(metadata, :quality_status)
    write_outcome = metadata_value(metadata, :write_outcome)
    checks = metadata |> metadata_value(:check_results) |> normalize_checks()

    if checks == [] and write_outcome not in [:no_op, "no_op"] do
      nil
    else
      failed? = failed_status?(status) or quality_status in [:failed, "failed"]
      warning? = quality_status in [:warning, "warning"]
      no_op? = write_outcome in [:no_op, "no_op"]

      {title, tone, quality_label, description} =
        cond do
          failed? and write_outcome in [:unknown, "unknown"] ->
            {"Check diagnostics from failed attempt", :error, "Outcome unknown",
             "The transaction outcome is unknown; verify the target state before retrying."}

          failed? and write_outcome in [:not_started, "not_started"] ->
            {"SQL checks not run", :error, "Not started",
             "The transaction did not begin, so no write was attempted."}

          failed? ->
            {"Rolled-back check diagnostics", :error, "Failed",
             "These results belong to a failed attempt and were not committed."}

          warning? and no_op? ->
            {"SQL quality checks", :warning, "Warning",
             "The existing target was kept and the quality warning was reported."}

          warning? ->
            {"SQL quality checks", :warning, "Warning",
             "The write committed with quality warnings."}

          no_op? ->
            {"SQL quality checks", :info, "Passed", "The existing target was kept unchanged."}

          true ->
            {"SQL quality checks", :success, "Passed", nil}
        end

      %{
        title: title,
        tone: tone,
        quality_label: quality_label,
        quality_status: value_string(quality_status || if(failed?, do: :failed, else: :passed)),
        write_label: if(no_op?, do: "No-op write", else: nil),
        write_outcome: value_string(write_outcome),
        description: description,
        checks: checks
      }
    end
  end

  defp check_summary(_metadata, _status), do: nil

  defp write_headline(_rows, outcome, _status) when outcome in [:not_started, "not_started"],
    do: {"No write was attempted", :error}

  defp write_headline(_rows, outcome, _status) when outcome in [:unknown, "unknown"],
    do: {"Write outcome unknown", :error}

  defp write_headline(_rows, outcome, _status) when outcome in [:no_op, "no_op"],
    do: {"Kept the existing table", :info}

  defp write_headline(rows, _outcome, status) when is_integer(rows) do
    tone = if failed_status?(status), do: :error, else: :success
    {"Wrote #{thousands(rows)} #{if rows == 1, do: "row", else: "rows"}", tone}
  end

  # A SQL asset reports `write_outcome: "written"` and no row count at all — the
  # count is not something the materialisation returns. Saying "Table written"
  # and naming the relation is the whole of what the metadata knows, and it is
  # still the answer to "did the data come out right?".
  defp write_headline(_rows, outcome, status)
       when outcome in [:written, :committed, "written", "committed"] do
    tone = if failed_status?(status), do: :error, else: :success
    {"Table written", tone}
  end

  defp write_headline(_rows, _outcome, _status), do: nil

  defp row_count(metadata) do
    Enum.find_value([:rows_written, :rows_affected, :rows_inserted], fn key ->
      case metadata_value(metadata, key) do
        rows when is_integer(rows) -> rows
        _absent -> nil
      end
    end)
  end

  defp write_target(metadata) do
    materialized_target(metadata_value(metadata, :materialized)) ||
      Enum.find_value([:relation, :table], fn key ->
        case metadata_value(metadata, key) do
          value when is_binary(value) and value != "" -> value
          _other -> nil
        end
      end)
  end

  defp materialized_target(materialized) when is_map(materialized) do
    [:schema, :name]
    |> Enum.map(&metadata_value(materialized, &1))
    |> Enum.reject(&(!is_binary(&1) or &1 == ""))
    |> case do
      [] -> nil
      parts -> Enum.join(parts, ".")
    end
  end

  defp materialized_target(_materialized), do: nil

  defp checks_label([]), do: nil

  defp checks_label(checks) do
    failed = Enum.count(checks, &(&1.tone == :error))
    warned = Enum.count(checks, &(&1.tone == :warning))
    total = length(checks)

    cond do
      failed > 0 -> %{label: "#{failed} of #{total} checks failed", tone: :error}
      warned > 0 -> %{label: "#{warned} of #{total} checks warned", tone: :warning}
      total == 1 -> %{label: "1 check passed", tone: :success}
      true -> %{label: "#{total} checks passed", tone: :success}
    end
  end

  defp thousands(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  defp normalize_checks(checks) when is_list(checks), do: Enum.map(checks, &normalize_check/1)
  defp normalize_checks(_checks), do: []

  defp normalize_check(check) when is_map(check) do
    outcome = metadata_value(check, :outcome)
    duration_ms = metadata_value(check, :duration_ms)

    %{
      name: check |> metadata_value(:name) |> value_string(),
      phase_label: check |> metadata_value(:phase) |> humanize_value(),
      outcome: value_string(outcome),
      outcome_label: humanize_value(outcome),
      origin: check |> metadata_value(:origin) |> value_string(),
      origin_label: check |> metadata_value(:origin) |> check_origin_label(),
      claim_id: metadata_value(check, :claim_id),
      tone: check_outcome_tone(outcome),
      message: metadata_value(check, :message),
      duration_label: if(is_integer(duration_ms), do: " · #{duration_ms} ms", else: ""),
      metrics: check |> metadata_value(:metrics) |> normalize_metrics()
    }
  end

  defp normalize_check(check) do
    %{
      name: value_string(check),
      phase_label: "Unknown phase",
      outcome: "unknown",
      outcome_label: "Unknown",
      origin: "authored",
      origin_label: "Custom",
      claim_id: nil,
      tone: :neutral,
      message: nil,
      duration_label: "",
      metrics: []
    }
  end

  defp normalize_metrics(metrics) when is_map(metrics) do
    metrics
    |> Enum.map(fn {key, value} -> {key_string(key), value_label(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp normalize_metrics(_metrics), do: []

  defp check_origin_label(origin) when origin in [:contract, "contract"], do: "Contract"
  defp check_origin_label(_origin), do: "Custom"

  defp check_origin_badge("contract"), do: "badge badge-info badge-soft badge-xs"
  defp check_origin_badge(_origin), do: "badge badge-ghost badge-xs"

  defp metadata_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp check_outcome_tone(outcome) when outcome in [:warned, "warned"], do: :warning

  defp check_outcome_tone(outcome)
       when outcome in [:failed, :errored, "failed", "errored"],
       do: :error

  defp check_outcome_tone(outcome)
       when outcome in [:passed, "passed", :materialization_skipped, "materialization_skipped"],
       do: :success

  defp check_outcome_tone(_outcome), do: :neutral

  defp check_summary_class(:error), do: "border-error/30 bg-error/10 text-error"
  defp check_summary_class(:warning), do: "border-warning/30 bg-warning/10 text-warning"
  defp check_summary_class(:info), do: "border-info/30 bg-info/10 text-info"
  defp check_summary_class(_tone), do: "border-success/30 bg-success/10 text-success"

  defp check_badge_class(:error), do: "badge badge-error badge-sm"
  defp check_badge_class(:warning), do: "badge badge-warning badge-sm"
  defp check_badge_class(:success), do: "badge badge-success badge-sm"
  defp check_badge_class(_tone), do: "badge badge-ghost badge-sm"

  defp humanize_value(nil), do: "Unknown"

  defp humanize_value(value) do
    value
    |> value_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp value_string(nil), do: ""
  defp value_string(value) when is_atom(value), do: Atom.to_string(value)
  defp value_string(value) when is_binary(value), do: value
  defp value_string(value), do: inspect(value)

  defp flatten_map(map), do: flatten_map(map, nil)

  defp flatten_map(map, prefix) do
    Enum.flat_map(map, fn {key, value} ->
      key = joined_key(prefix, key)

      case value do
        %_{} = scalar -> [{key, scalar}]
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
