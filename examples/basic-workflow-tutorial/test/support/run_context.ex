defmodule CrmDemo.RunContext do
  @moduledoc """
  Builds the run context Favn would normally supply, so assets can be exercised
  without an orchestrator.
  """

  alias Favn.Run.Context
  alias Favn.Window.{Key, Runtime}

  @doc "Builds a context for one asset ref, optionally with `:window` and `:attempt`."
  @spec new(module() | Favn.Ref.t(), keyword()) :: Context.t()
  def new(asset_ref, opts \\ []) do
    {:ok, asset} = Favn.get_asset(asset_ref)

    %Context{
      run_id: Keyword.get(opts, :run_id, "run_test"),
      asset: asset,
      runtime_config: %{crm_api: %{base_url: "https://crm.test/v1", token: "test-token"}},
      params: %{},
      window: Keyword.get(opts, :window),
      run_started_at: DateTime.utc_now(),
      stage: 0,
      attempt: Keyword.get(opts, :attempt, 1),
      max_attempts: 1
    }
  end

  @doc "Builds the UTC daily window for a date."
  @spec day(Date.t()) :: Runtime.t()
  def day(date) do
    start_at = DateTime.new!(date, ~T[00:00:00], "Etc/UTC")

    Runtime.new!(
      :day,
      start_at,
      DateTime.add(start_at, 1, :day),
      Key.new!(:day, start_at, "Etc/UTC")
    )
  end
end
