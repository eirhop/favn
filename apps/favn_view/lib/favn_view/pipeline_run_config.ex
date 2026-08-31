defmodule FavnView.PipelineRunConfig do
  @moduledoc """
  What the pipeline run dialog submits, and the rules it must satisfy.

  A pipeline declares how it runs — its window kind, its timezone, whether
  adjacent windows are combined. The dialog opens on those declared values and an
  operator may deviate from them for one submission; nothing here is persisted,
  and nothing here edits the manifest.

  Every field round-trips through an HTML form, so the shape is strings, and
  `default/1` and `from_params/2` are the only two ways to build one.

  ## The period decides which command runs

  An empty period submits no window at all, which is how the control plane is
  asked for the latest complete period. A single period submits that one window.
  A range is a backfill, which is a different command with a different request —
  so `range_requested?/1` answers it once, rather than each call site reading
  `:to` and eventually disagreeing about what a blank one means.

  `combine_windows` only reaches the backfill command, so it is meaningless
  without a range. The dialog shows it only when one is asked for.

  ## Examples

      iex> alias FavnView.PipelineRunConfig
      iex> PipelineRunConfig.validate(PipelineRunConfig.default(nil), false)
      nil

      iex> alias FavnView.PipelineRunConfig
      iex> config = %{PipelineRunConfig.default(nil) | to: "2026-08", kind: "month"}
      iex> PipelineRunConfig.validate(config, true)
      "A range needs a period to start from."
  """

  @refresh_choices ~w(auto missing force_all)
  @window_kind_choices ~w(hour day month year)
  @timezone_pattern ~r/\A[A-Za-z0-9_+\-\/]{1,64}\z/

  @typedoc "A run configuration as the dialog's form carries it."
  @type t :: %{
          required(:kind) => String.t(),
          required(:from) => String.t(),
          required(:to) => String.t(),
          required(:refresh) => String.t(),
          required(:combine_windows) => boolean(),
          required(:timezone) => String.t()
        }

  @doc """
  The configuration a pipeline's dialog opens on.

  Takes the pipeline's declared window policy, so the dialog opens on what the
  manifest says rather than on a house default. A pipeline with no window policy
  still gets a kind and a timezone, because a form field needs a value; they are
  never submitted, since a pipeline without a window has no period to run.

  ## Examples

      iex> FavnView.PipelineRunConfig.default(%{window: %{"kind" => "day", "timezone" => "Europe/Oslo"}})
      %{combine_windows: false, from: "", kind: "day", refresh: "auto", timezone: "Europe/Oslo", to: ""}
  """
  @spec default(map() | nil) :: t()
  def default(%{window: window}) when is_map(window) do
    %{
      kind: window |> field(:kind) |> kind_value(),
      from: "",
      to: "",
      refresh: "auto",
      combine_windows: window |> field(:combine_windows) |> boolean_value(),
      timezone: window |> field(:timezone) |> timezone_value()
    }
  end

  def default(_pipeline) do
    %{
      kind: "month",
      from: "",
      to: "",
      refresh: "auto",
      combine_windows: false,
      timezone: "Etc/UTC"
    }
  end

  @doc """
  Applies one form change to the current configuration.

  A field the form did not send keeps its current value rather than blanking:
  the period controls live behind a disclosure and the combine checkbox appears
  only once a range is asked for, so a payload that omits one of them describes a
  form that did not render it, not an operator who cleared it.

  The window kind is never taken from params. A pipeline declares one, the
  control plane refuses a submission naming another, and the form offers no way
  to change it — so a kind arriving in a payload is not an operator's choice.
  """
  @spec from_params(map(), t()) :: t()
  def from_params(%{"run_config" => params}, current) when is_map(params) do
    %{
      kind: current.kind,
      from: params |> Map.get("from", current.from) |> trim(),
      to: params |> Map.get("to", current.to) |> trim(),
      refresh: Map.get(params, "refresh", current.refresh),
      combine_windows:
        params
        |> Map.get("combine_windows", to_string(current.combine_windows))
        |> boolean_value(),
      timezone: params |> Map.get("timezone", current.timezone) |> trim()
    }
  end

  def from_params(_params, current), do: current

  @doc """
  Returns why this configuration cannot be submitted, or `nil` when it can.

  Answers for the form alone. The control plane revalidates everything it cares
  about; a rule here exists so an operator sees the problem before submitting
  rather than as a rejected run.
  """
  @spec validate(t(), boolean()) :: String.t() | nil
  def validate(config, windowed?) do
    cond do
      config.refresh not in @refresh_choices ->
        "Refresh choice is invalid."

      not windowed? and period_requested?(config) ->
        "This pipeline has no window, so it cannot run a period."

      period_requested?(config) and config.kind not in @window_kind_choices ->
        "Window kind is invalid."

      range_requested?(config) and blank?(config.from) ->
        "A range needs a period to start from."

      period_requested?(config) and not valid_timezone?(config.timezone) ->
        "Timezone is invalid."

      true ->
        nil
    end
  end

  @doc """
  Whether this configuration names a period at all.

  ## Examples

      iex> FavnView.PipelineRunConfig.period_requested?(%{from: "", to: ""})
      false

      iex> FavnView.PipelineRunConfig.period_requested?(%{from: "2026-03", to: ""})
      true
  """
  @spec period_requested?(map()) :: boolean()
  def period_requested?(config),
    do: not blank?(Map.get(config, :from)) or not blank?(Map.get(config, :to))

  @doc """
  Whether this configuration runs a range of periods rather than one.

  ## Examples

      iex> FavnView.PipelineRunConfig.range_requested?(%{to: "   "})
      false

      iex> FavnView.PipelineRunConfig.range_requested?(%{to: "2026-08"})
      true
  """
  @spec range_requested?(map()) :: boolean()
  def range_requested?(config), do: not blank?(Map.get(config, :to))

  @doc """
  The fields this configuration changes from what the pipeline declares.

  The dialog marks each changed row, so a deviation cannot hide inside a closed
  disclosure, and offers a reset only when there is something to reset.

  ## Examples

      iex> alias FavnView.PipelineRunConfig
      iex> default = PipelineRunConfig.default(nil)
      iex> PipelineRunConfig.changed_fields(%{default | refresh: "force_all"}, default)
      [:refresh]
  """
  @spec changed_fields(t(), t()) :: [:period | :refresh | :combine_windows]
  def changed_fields(config, default) do
    [
      period_requested?(config) && :period,
      config.refresh != default.refresh && :refresh,
      range_requested?(config) && config.combine_windows != default.combine_windows &&
        :combine_windows
    ]
    |> Enum.filter(& &1)
  end

  @doc "Whether this configuration recomputes periods that already succeeded."
  @spec forces?(map()) :: boolean()
  def forces?(config), do: Map.get(config, :refresh) == "force_all"

  @doc """
  The window request for a single-period run.

  ## Examples

      iex> FavnView.PipelineRunConfig.window_request(%{kind: "month", from: "2026-03", timezone: "Etc/UTC"})
      %{kind: "month", timezone: "Etc/UTC", value: "2026-03"}
  """
  @spec window_request(t()) :: map()
  def window_request(config),
    do: %{kind: config.kind, value: config.from, timezone: config.timezone}

  @doc """
  The range request for a backfill.

  ## Examples

      iex> config = %{kind: "month", from: "2026-01", to: "2026-08", timezone: "Etc/UTC"}
      iex> FavnView.PipelineRunConfig.range_request(config)
      %{from: "2026-01", kind: "month", timezone: "Etc/UTC", to: "2026-08"}
  """
  @spec range_request(t()) :: map()
  def range_request(config),
    do: %{kind: config.kind, from: config.from, to: config.to, timezone: config.timezone}

  defp field(window, key), do: Map.get(window, key) || Map.get(window, Atom.to_string(key))

  defp kind_value(value) when value in [:hour, :day, :month, :year], do: Atom.to_string(value)
  defp kind_value(value) when value in ["hour", "day", "month", "year"], do: value
  defp kind_value(:hourly), do: "hour"
  defp kind_value(:daily), do: "day"
  defp kind_value(:monthly), do: "month"
  defp kind_value(:yearly), do: "year"
  defp kind_value("hourly"), do: "hour"
  defp kind_value("daily"), do: "day"
  defp kind_value("monthly"), do: "month"
  defp kind_value("yearly"), do: "year"
  defp kind_value(_value), do: "month"

  defp timezone_value(value) when is_binary(value) and value != "", do: value
  defp timezone_value(_value), do: "Etc/UTC"

  defp boolean_value(true), do: true
  defp boolean_value("true"), do: true
  defp boolean_value(_value), do: false

  defp trim(value) when is_binary(value), do: String.trim(value)
  defp trim(_value), do: ""

  defp blank?(value), do: not is_binary(value) or String.trim(value) == ""

  defp valid_timezone?(value) when is_binary(value), do: String.match?(value, @timezone_pattern)
  defp valid_timezone?(_value), do: false
end
