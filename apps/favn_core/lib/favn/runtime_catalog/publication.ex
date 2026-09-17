defmodule Favn.RuntimeCatalog.Publication do
  @moduledoc """
  Bounded publication intent pinned before a managed SQL asset is dispatched.

  This is target evidence, not a run result. Expiry is calculated from the pinned
  freshness key and the publication instant; it never schedules execution.
  """
  alias Favn.Contracts.RunnerWork
  alias Favn.Freshness.{Key, Policy}
  alias Favn.Manifest.{Asset, Serializer, TargetDescriptor}
  alias Favn.Semantic.Snapshot
  alias Favn.TimePeriod
  alias Favn.Window.Runtime

  @version 1
  @max_bytes 65_536
  @fields [
    :workspace_id,
    :publication_id,
    :target_id,
    :generation_id,
    :asset_ref,
    :run_id,
    :step_id,
    :attempt,
    :manifest_id,
    :manifest_hash,
    :runner_release,
    :freshness_key,
    :policy,
    :windows,
    :coverage,
    :candidate
  ]
  @enforce_keys @fields
  defstruct @fields ++ [version: @version]
  @type t :: %__MODULE__{}

  @doc "Whether the pinned descriptor requires native runtime publication."
  @spec supported?(term()) :: boolean()
  def supported?(%TargetDescriptor{adapter: "Elixir.Favn.SQL.Adapter.DuckDB.ADBC"}), do: true
  def supported?(_), do: false

  @doc "Builds the bounded intent from immutable work and the planner's freshness key."
  @spec new(Asset.t(), RunnerWork.t(), String.t(), String.t()) ::
          {:ok, t() | nil} | {:error, atom()}
  def new(%Asset{} = asset, %RunnerWork{} = work, workspace, freshness_key) do
    if supported?(asset.target_descriptor) do
      with {:ok, windows} <- windows(RunnerWork.window(work)) do
        publication = %__MODULE__{
          workspace_id: workspace,
          publication_id:
            Snapshot.digest("rp_", [workspace, work.run_id, work.asset_step_id, work.attempt]),
          target_id: work.logical_target_id,
          generation_id: work.target_generation_id,
          asset_ref: Snapshot.ref(asset.ref),
          run_id: work.run_id,
          step_id: work.asset_step_id,
          attempt: work.attempt,
          manifest_id: work.manifest_version_id,
          manifest_hash: work.manifest_content_hash,
          runner_release: work.required_runner_release_id,
          freshness_key: freshness_key,
          policy: json(asset.freshness),
          windows: windows,
          coverage: json(asset.coverage),
          candidate: work.target_operation == :rebuild_candidate
        }

        with :ok <- validate(publication), do: {:ok, publication}
      end
    else
      {:ok, nil}
    end
  end

  @doc "Validates persisted publication intents without loading customer modules."
  @spec validate(term()) :: :ok | {:error, atom()}
  def validate(nil), do: :ok

  def validate(%__MODULE__{version: @version} = p) do
    identifiers = Map.take(p, @fields -- [:policy, :windows, :coverage, :candidate, :attempt])

    with true <- Enum.all?(identifiers, fn {_, v} -> is_binary(v) and byte_size(v) in 1..4096 end),
         true <- is_integer(p.attempt) and p.attempt > 0 and is_boolean(p.candidate),
         {:ok, _} <- Key.parse(p.freshness_key),
         {:ok, _} <- Policy.from_value(p.policy),
         true <- is_list(p.windows) and length(p.windows) <= 1000,
         true <- Enum.all?(p.windows, &valid_window?/1),
         true <- byte_size(Serializer.encode_canonical!(Map.from_struct(p))) <= @max_bytes do
      :ok
    else
      _ -> {:error, :invalid_runtime_publication}
    end
  rescue
    _ -> {:error, :invalid_runtime_publication}
  end

  def validate(_), do: {:error, :invalid_runtime_publication}

  @doc "Returns expiry kind, optional UTC deadline and whether equality is fresh."
  @spec expiry(t(), DateTime.t()) ::
          {:ok, {String.t(), DateTime.t() | nil, boolean()}} | {:error, atom()}
  def expiry(%__MODULE__{} = p, now) do
    with {:ok, policy} <- Policy.from_value(p.policy), {:ok, key} <- Key.parse(p.freshness_key) do
      deadline(policy, key, now)
    else
      _ -> {:error, :invalid_runtime_freshness}
    end
  end

  defp deadline(nil, _, _), do: {:ok, {"unknown", nil, false}}
  defp deadline(%Policy{mode: :always}, _, _), do: {:ok, {"always", nil, false}}

  defp deadline(%Policy{mode: :max_age, amount: n, unit: unit}, _, now) do
    seconds = n * Map.fetch!(%{second: 1, minute: 60, hour: 3600, day: 86400}, unit)
    {:ok, {"deadline", DateTime.add(now, seconds), true}}
  end

  defp deadline(%Policy{mode: :window_success}, :latest, _), do: {:ok, {"none", nil, false}}

  defp deadline(%Policy{mode: :window_success}, {:window, _}, _), do: {:ok, {"none", nil, false}}

  defp deadline(%Policy{mode: :window_success}, {:window_refresh, _, kind, tz, start}, _),
    do: calendar(kind, tz, start)

  defp deadline(%Policy{mode: :calendar_period}, {:calendar, kind, tz, start}, _),
    do: calendar(kind, tz, start)

  defp deadline(_, _, _), do: {:error, :invalid_runtime_freshness}

  defp calendar(kind, tz, start) do
    case TimePeriod.bounds(kind, start, tz) do
      {:ok, period} -> {:ok, {"deadline", period.end_at, false}}
      _ -> {:error, :invalid_runtime_freshness}
    end
  end

  @doc "Checks that a delete/insert mutation exactly matches the pinned logical windows."
  @spec validate_window(t() | nil, Runtime.t() | nil) :: :ok | {:error, atom()}
  def validate_window(nil, _), do: :ok

  def validate_window(%__MODULE__{} = p, window) do
    case windows(window) do
      {:ok, windows} when windows != [] and windows == p.windows -> :ok
      _ -> {:error, :invalid_runtime_coverage_scope}
    end
  end

  defp windows(nil), do: {:ok, []}

  defp windows(%Runtime{} = w) do
    with :ok <- Runtime.validate(w),
         true <- w.logical_window_count in 1..1000,
         true <-
           DateTime.diff(w.end_at, w.start_at) <=
             w.logical_window_count *
               Map.fetch!(
                 %{hour: 7200, day: 172_800, month: 32 * 86400, year: 367 * 86400},
                 w.kind
               ),
         {:ok, periods} <- TimePeriod.expand_range(w.kind, w.start_at, w.end_at, w.timezone),
         true <- length(periods) == w.logical_window_count,
         true <- DateTime.compare(hd(periods).start_at, w.start_at) == :eq,
         true <- DateTime.compare(List.last(periods).end_at, w.end_at) == :eq do
      {:ok,
       Enum.map(periods, fn p ->
         %{
           "kind" => to_string(p.kind),
           "timezone" => p.timezone,
           "start_at" => DateTime.to_iso8601(p.start_at),
           "end_at" => DateTime.to_iso8601(p.end_at)
         }
       end)}
    else
      _ -> {:error, :invalid_runtime_coverage_scope}
    end
  end

  defp valid_window?(%{"kind" => k, "timezone" => tz, "start_at" => s, "end_at" => e} = w) do
    with true <- map_size(w) == 4 and k in ~w(hour day month year),
         {:ok, start_at, _} <- DateTime.from_iso8601(s),
         {:ok, end_at, _} <- DateTime.from_iso8601(e),
         {:ok, boundary} <-
           TimePeriod.floor(
             start_at,
             Map.fetch!(%{"hour" => :hour, "day" => :day, "month" => :month, "year" => :year}, k),
             tz
           ),
         {:ok, next} <-
           TimePeriod.shift(
             boundary,
             Map.fetch!(%{"hour" => :hour, "day" => :day, "month" => :month, "year" => :year}, k),
             1
           ) do
      DateTime.compare(start_at, boundary) == :eq and DateTime.compare(end_at, next) == :eq
    else
      _ -> false
    end
  end

  defp valid_window?(_), do: false
  defp json(value), do: value |> Serializer.encode_manifest!() |> Jason.decode!()
end
