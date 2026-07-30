defmodule FavnView.Dev.DesignSystem.Fixtures.Timeline do
  @moduledoc """
  Timeline window and run-config view models.

  Three surfaces render the same window shape — the asset detail page, the window
  timeline panel, and the selected-window actions — so the shape is built once
  here. It is a view model, not domain data: `favn_view` never reaches past the
  orchestrator facade, and these fixtures reach nowhere at all.

  Compiled in `:dev` and `:test`, so a LiveView test and the design system can
  assert against the same window.
  """

  @doc """
  A timeline window.

  Errored windows carry the latest failed run and the config it used, which is
  what the run-config panel prefills from.
  """
  @spec window(String.t(), atom(), atom(), String.t(), String.t(), atom()) :: map()
  def window(id, source, kind, value, label, status) do
    %{
      id: id,
      source: source,
      kind: kind,
      value: value,
      timezone: "Etc/UTC",
      label: label,
      date_label: "#{label}, 2026",
      range_label: "#{label}, 2026",
      status: status,
      status_label: status_label(status),
      latest_run_id: if(status == :error, do: "run_failed_window"),
      latest_run_status: if(status == :error, do: :error),
      latest_run_config:
        if(status == :error, do: run_config(source, kind, value, "none", "force_all")),
      run_enabled?: true,
      run_disabled_reason: nil,
      run_label: "Run asset",
      default_run_config: run_config(source, kind, value)
    }
  end

  @doc """
  A window on the refresh timeline.
  """
  @spec refresh_window(String.t(), String.t(), atom()) :: map()
  def refresh_window(value, label, status) do
    window("refresh:day:#{value}", :refresh_timeline, :day, value, label, status)
  end

  @doc """
  A window on the data-coverage timeline.
  """
  @spec data_window(String.t(), String.t(), atom()) :: map()
  def data_window(value, label, status) do
    window("window:day:#{value}", :data_coverage_timeline, :day, value, label, status)
  end

  @doc """
  The refresh timeline: fresh, failed, running, fresh.
  """
  @spec refresh_timeline() :: [map()]
  def refresh_timeline do
    [
      refresh_window("2026-06-09", "Jun 9", :success),
      refresh_window("2026-06-10", "Jun 10", :error),
      refresh_window("2026-06-11", "Jun 11", :warning),
      refresh_window("2026-06-12", "Jun 12", :success)
    ]
  end

  @doc """
  The data-coverage timeline, including a missing window.
  """
  @spec data_coverage_timeline() :: [map()]
  def data_coverage_timeline do
    [
      data_window("2026-06-09", "Jun 9", :success),
      data_window("2026-06-10", "Jun 10", :error),
      data_window("2026-06-11", "Jun 11", :muted),
      data_window("2026-06-12", "Jun 12", :success)
    ]
  end

  @doc """
  The freshness timeline: the refresh windows, none of which can be run directly.
  """
  @spec freshness_timeline() :: [map()]
  def freshness_timeline do
    Enum.map(refresh_timeline(), fn window ->
      %{
        window
        | id: String.replace_prefix(window.id, "refresh:", "freshness:"),
          source: :freshness_timeline,
          timezone: "Europe/Oslo",
          run_enabled?: false,
          run_disabled_reason: :freshness_period_not_runnable,
          run_label: nil
      }
    end)
  end

  @doc """
  A run config for a window.
  """
  @spec run_config(atom(), atom(), String.t(), String.t(), String.t()) :: map()
  def run_config(source, kind, value, dependencies \\ "all", refresh \\ "auto") do
    %{
      dependencies: dependencies,
      refresh: refresh,
      source: Atom.to_string(source),
      kind: Atom.to_string(kind),
      value: value,
      timezone: "Etc/UTC"
    }
  end

  @doc """
  The run config for an asset with no windows at all.
  """
  @spec full_refresh_run_config() :: map()
  def full_refresh_run_config do
    %{dependencies: "all", refresh: "auto", source: nil, kind: "", value: "", timezone: "Etc/UTC"}
  end

  @doc """
  The default refresh run config the fixtures select.
  """
  @spec default_run_config() :: map()
  def default_run_config, do: run_config(:refresh_timeline, :day, "2026-06-12")

  @doc """
  The operator-facing label for a window status.
  """
  @spec status_label(atom()) :: String.t()
  def status_label(:success), do: "Fresh"
  def status_label(:warning), do: "Running"
  def status_label(:error), do: "Failed"
  def status_label(:muted), do: "Missing"
end
