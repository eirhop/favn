defmodule FavnView.StatusConcerns do
  @moduledoc """
  Turns orchestrator reads into the concerns the status screen renders.

  A *concern* is something an operator would act on, stated in their language,
  with the one action that addresses it. This module decides what counts, how it
  is worded, and in what order — all as pure functions over already-fetched
  results, so the wording is testable without a socket, a browser, or a database.

  `FavnView.StatusLive` does the fetching and nothing else.

  ## Partial failure is not failure

  `build/1` takes each source's result, `{:ok, _}` or `{:error, _}`, and reports
  the sources it could not use in `:unavailable` rather than failing. A broken
  schedule backend must not hide a failing run.

  ## What does not count

  Deciding what to leave out matters more than what to include, because a page
  that lists everything is the catalogue again:

    * an asset that has never run is new, not broken, so `:unknown` health is
      not a concern;
    * a *failed* rebuild is a decision an operator already made, while an
      *unknown* one is a question the system cannot answer alone — only the
      latter appears;
    * a schedule that is simply disabled was disabled on purpose.
  """

  alias FavnView.AssetRoute
  alias FavnView.ScheduleRoute

  @type source_results :: %{
          required(:runs) => term(),
          required(:assets) => term(),
          required(:schedules) => term(),
          required(:rebuilds) => term()
        }

  @type result :: %{groups: [map()], unavailable: [String.t()]}

  @doc """
  Builds the concern groups, dropping empty groups and naming unusable sources.

  ## Examples

      iex> alias FavnView.StatusConcerns
      iex> StatusConcerns.build(%{runs: {:ok, %{items: []}}, assets: {:ok, []}, schedules: {:ok, %{items: []}}, rebuilds: {:ok, %{items: []}}})
      %{groups: [], unavailable: []}

      iex> alias FavnView.StatusConcerns
      iex> result = StatusConcerns.build(%{runs: {:error, :unavailable}, assets: {:ok, []}, schedules: {:ok, %{items: []}}, rebuilds: {:ok, %{items: []}}})
      iex> result.unavailable
      ["Runs"]
  """
  @spec build(source_results(), keyword()) :: result()
  def build(sources, opts \\ []) do
    limit = Keyword.get(opts, :limit, 8)

    [
      {"Runs", &run_group(&1, limit), Map.get(sources, :runs)},
      {"Assets", &asset_group(&1, limit), Map.get(sources, :assets)},
      {"Schedules", &schedule_group(&1, limit), Map.get(sources, :schedules)},
      {"Rebuilds", &rebuild_group(&1, limit), Map.get(sources, :rebuilds)}
    ]
    |> Enum.reduce(%{groups: [], unavailable: []}, fn {source, builder, result}, acc ->
      case result do
        {:ok, payload} -> %{acc | groups: acc.groups ++ [builder.(payload)]}
        _other -> %{acc | unavailable: acc.unavailable ++ [source]}
      end
    end)
    |> then(fn result -> %{result | groups: Enum.reject(result.groups, &(&1.concerns == []))} end)
  end

  defp run_group(payload, limit) do
    %{
      id: :failing_runs,
      title: "Failing runs",
      description: "A run reached a terminal state with failed work in it.",
      icon: "hero-exclamation-triangle",
      tone: :error,
      concerns: payload |> items() |> Enum.take(limit) |> Enum.map(&run_concern/1)
    }
  end

  defp run_concern(item) do
    id = Map.get(item, :root_run_id) || Map.get(item, :id)

    %{
      id: "run-#{id}",
      title: run_title(item, id),
      detail: run_detail(item),
      meta: timestamp_label(Map.get(item, :started_at) || Map.get(item, :inserted_at)),
      action_label: "Open run",
      action_path: "/runs/#{id}"
    }
  end

  defp run_title(item, id) do
    case Map.get(item, :trigger_label) || Map.get(item, :target_label) do
      nil -> "Run #{short_id(id)}"
      label -> to_string(label)
    end
  end

  defp run_detail(item) do
    failed = Map.get(item, :failed_count) || Map.get(item, :failures)
    total = Map.get(item, :total_count) || Map.get(item, :asset_count)

    cond do
      is_integer(failed) and failed > 0 and is_integer(total) ->
        "#{failed} of #{total} assets failed."

      is_integer(failed) and failed > 0 ->
        "#{failed} assets failed."

      true ->
        "The run finished with failures."
    end
  end

  defp asset_group(entries, limit) do
    %{
      id: :assets,
      title: "Assets needing attention",
      description: "Health, coverage, or target compatibility is not clear.",
      icon: "hero-sparkles",
      tone: :warning,
      concerns:
        entries
        |> Enum.filter(&asset_concern?/1)
        |> Enum.take(limit)
        |> Enum.map(&asset_concern/1)
    }
  end

  @doc """
  Whether a catalogue entry is something an operator should look at.

  ## Examples

      iex> FavnView.StatusConcerns.asset_concern?(%{status: :healthy})
      false

      iex> FavnView.StatusConcerns.asset_concern?(%{status: :unknown})
      false

      iex> FavnView.StatusConcerns.asset_concern?(%{status: :failed})
      true

      iex> FavnView.StatusConcerns.asset_concern?(%{status: :healthy, coverage: %{status: :incomplete}})
      true
  """
  @spec asset_concern?(map()) :: boolean()
  def asset_concern?(entry) do
    Map.get(entry, :status) in [:failed, :stale, :degraded] or
      get_in(entry, [:coverage, Access.key(:status)]) == :incomplete or
      get_in(entry, [:compatibility, Access.key(:status)]) in [:incompatible, :rebuild_required]
  end

  defp asset_concern(entry) do
    target_id = Map.fetch!(entry, :target_id)

    %{
      id: "asset-#{target_id}",
      title: asset_name(entry),
      detail: asset_detail(entry),
      tone: if(Map.get(entry, :status) == :failed, do: :error, else: :warning),
      meta: nil,
      action_label: "Open asset",
      action_path: "/assets/#{AssetRoute.to_param(target_id)}"
    }
  end

  defp asset_detail(entry) do
    [
      health_detail(Map.get(entry, :status)),
      coverage_detail(get_in(entry, [:coverage, Access.key(:status)])),
      compatibility_detail(get_in(entry, [:compatibility, Access.key(:status)]))
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp health_detail(:failed), do: "The last run failed."
  defp health_detail(:stale), do: "Later than its freshness policy allows."
  defp health_detail(:degraded), do: "Running with reduced health."
  defp health_detail(_status), do: nil

  defp coverage_detail(:incomplete), do: "Declared windows are missing."
  defp coverage_detail(_status), do: nil

  defp compatibility_detail(:incompatible), do: "The target no longer matches the contract."
  defp compatibility_detail(:rebuild_required), do: "The target must be rebuilt before writes."
  defp compatibility_detail(_status), do: nil

  defp asset_name(entry) do
    relation = Map.get(entry, :relation) || %{}

    Map.get(relation, :name) || Map.get(entry, :name) || to_string(Map.fetch!(entry, :target_id))
  end

  defp schedule_group(payload, limit) do
    %{
      id: :schedules,
      title: "Schedules not running",
      description: "A schedule errored, or is waiting for an operator to activate it.",
      icon: "hero-calendar-days",
      tone: :warning,
      concerns:
        payload
        |> items()
        |> Enum.filter(&schedule_concern?/1)
        |> Enum.take(limit)
        |> Enum.map(&schedule_concern/1)
    }
  end

  @doc """
  Whether a schedule entry is stopping work from happening.
  """
  @spec schedule_concern?(map()) :: boolean()
  def schedule_concern?(entry) do
    Map.get(entry, :last_scheduler_error) != nil or
      Map.get(entry, :activation_state) in [:pending_activation, :needs_review]
  end

  defp schedule_concern(entry) do
    %{
      id: "schedule-#{entry.id}",
      title: schedule_title(entry),
      detail: schedule_detail(entry),
      tone: if(Map.get(entry, :last_scheduler_error), do: :error, else: :warning),
      meta: nil,
      action_label: "Open schedule",
      action_path: "/schedules/#{ScheduleRoute.to_param(entry.id)}"
    }
  end

  defp schedule_title(entry) do
    case Map.get(entry, :pipeline_module) do
      nil -> to_string(entry.id)
      module -> module |> to_string() |> String.replace_prefix("Elixir.", "")
    end
  end

  defp schedule_detail(%{last_scheduler_error: error}) when is_map(error) do
    Map.get(error, :message, "The scheduler reported an error.")
  end

  defp schedule_detail(%{activation_state: :pending_activation}),
    do: "Never activated, so it has never run."

  defp schedule_detail(%{activation_state: :needs_review}),
    do: "Changed since it was activated and needs review."

  defp schedule_detail(_entry), do: "Not currently scheduled to run."

  defp rebuild_group(payload, limit) do
    %{
      id: :operations,
      title: "Operations to reconcile",
      description: "A rebuild ended in an unknown outcome and needs reconciling.",
      icon: "hero-arrow-path-rounded-square",
      tone: :warning,
      concerns:
        payload
        |> items()
        |> Enum.filter(&rebuild_concern?/1)
        |> Enum.take(limit)
        |> Enum.map(&rebuild_concern/1)
    }
  end

  @doc """
  Whether a rebuild's outcome is unresolved.

  ## Examples

      iex> FavnView.StatusConcerns.rebuild_concern?(%{state: :unknown})
      true

      iex> FavnView.StatusConcerns.rebuild_concern?(%{state: :failed})
      false

      iex> FavnView.StatusConcerns.rebuild_concern?(%{state: :succeeded})
      false
  """
  @spec rebuild_concern?(map()) :: boolean()
  def rebuild_concern?(item) do
    Map.get(item, :state) in [:unknown, :needs_reconciliation, :reconciling]
  end

  defp rebuild_concern(item) do
    %{
      id: "operation-#{item.id}",
      title: "Rebuild #{short_id(item.id)}",
      detail: "State is #{humanize(Map.get(item, :state))}; the outcome is not established.",
      meta: timestamp_label(Map.get(item, :updated_at) || Map.get(item, :inserted_at)),
      action_label: "Open rebuild",
      action_path: "/rebuilds/#{item.id}"
    }
  end

  defp items(%{items: items}) when is_list(items), do: items
  defp items(items) when is_list(items), do: items
  defp items(_other), do: []

  defp timestamp_label(%DateTime{} = datetime),
    do: FavnView.Time.format(datetime, "%b %-d %H:%M")

  defp timestamp_label(_value), do: nil

  defp short_id(id) when is_binary(id) and byte_size(id) > 12, do: String.slice(id, 0, 12)
  defp short_id(id), do: to_string(id)

  defp humanize(value), do: value |> to_string() |> String.replace("_", " ")
end
