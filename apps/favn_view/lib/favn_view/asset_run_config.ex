defmodule FavnView.AssetRunConfig do
  @moduledoc """
  The configuration the asset run dialog submits, and the rules it must satisfy.

  A run configuration exists in two shapes. The backend reports the period an asset
  is due for as atoms; the dialog is an HTML form, so every field it round-trips is a
  string. `from_asset/1` and `from_params/2` are the two ways in, and both produce the
  string shape — nothing else in the view should build one field by field.

  `validate/1` returns the message to show an operator, or `nil` when the
  configuration may be submitted. It answers for the form alone: the backend
  revalidates every rule it cares about, and a rule here exists so an operator sees
  the problem before submitting rather than as a rejected run.

  ## The period fields are conditional

  Rules about the period only apply when a period was asked for at all. An asset that
  replaces its whole relation submits no period, and demanding a kind or a timezone
  from it would refuse a configuration that is complete.

  ## Examples

      iex> FavnView.AssetRunConfig.validate(FavnView.AssetRunConfig.default())
      nil

      iex> config = %{FavnView.AssetRunConfig.default() | dependencies: "none",
      ...>                                                refresh: "force_selected_upstream"}
      iex> FavnView.AssetRunConfig.validate(config)
      "force_selected_upstream requires dependencies=all."
  """

  @dependency_choices ~w(all none)
  @refresh_choices ~w(auto missing force_selected force_selected_upstream force_all)
  @source_choices ~w(refresh_timeline data_coverage_timeline)
  @window_kind_choices ~w(hour day month year)
  @timezone_pattern ~r/\A[A-Za-z0-9_+\-\/]{1,64}\z/

  @typedoc "A run configuration as the dialog's form carries it."
  @type t :: %{
          required(:dependencies) => String.t(),
          required(:refresh) => String.t(),
          required(:source) => String.t() | nil,
          required(:kind) => String.t(),
          required(:value) => String.t(),
          required(:to) => String.t(),
          required(:timezone) => String.t()
        }

  @doc "The configuration a dialog opens on when the backend offers no period."
  @spec default() :: t()
  def default do
    %{
      dependencies: "all",
      refresh: "auto",
      source: nil,
      kind: "",
      value: "",
      to: "",
      timezone: "Etc/UTC"
    }
  end

  @doc """
  Builds the configuration an asset's dialog opens on.

  The period is the one the backend reports the asset is due for. It is absent when
  no single pipeline owns the asset, in which case the dialog opens on `default/0` —
  and the action that opens it is disabled anyway. Whether the period *fields* show is
  `has_data_windows?`, which the page decides.
  """
  @spec from_asset(map() | nil) :: t()
  def from_asset(%{default_run_config: config}) when is_map(config), do: from_backend(config)
  def from_asset(_asset), do: default()

  @doc """
  Applies one form change to the current configuration.

  A period field the form did not send keeps its current value, so a change event for
  one of them cannot blank the others. Those are exactly the fields that disable while
  a submission is in flight, and a disabled control is not serialized, so they can drop
  out of a payload for reasons that have nothing to do with tampering.

  The two radio groups fall back to their default instead. They never disable, so the
  only way they go missing is that neither option is checked — which happens when the
  current answer is not one the dialog renders, and then the default is the right
  answer rather than the unrenderable one.
  """
  @spec from_params(map(), t()) :: t()
  def from_params(%{"run_config" => params}, current) when is_map(params) do
    %{
      dependencies: Map.get(params, "dependencies", "all"),
      refresh: Map.get(params, "refresh", "auto"),
      source: Map.get(params, "source", Map.get(current, :source)),
      kind: Map.get(params, "kind", Map.get(current, :kind, "")),
      value: Map.get(params, "value", Map.get(current, :value, "")),
      to: Map.get(params, "to", Map.get(current, :to, "")),
      timezone: Map.get(params, "timezone", Map.get(current, :timezone, "Etc/UTC"))
    }
  end

  def from_params(_params, current), do: current

  @doc """
  Returns why this configuration cannot be submitted, or `nil` when it can.
  """
  @spec validate(t()) :: String.t() | nil
  def validate(config) do
    cond do
      config.dependencies not in @dependency_choices ->
        "Dependency choice is invalid."

      config.refresh not in @refresh_choices ->
        "Refresh choice is invalid."

      config.dependencies == "none" and config.refresh == "force_selected_upstream" ->
        "force_selected_upstream requires dependencies=all."

      window_context_requested?(config) and config.source not in @source_choices ->
        "Window source is invalid."

      window_context_requested?(config) and config.kind not in @window_kind_choices ->
        "Window kind is invalid."

      window_context_requested?(config) and blank?(config.value) ->
        "Window range start is required."

      window_context_requested?(config) and not valid_timezone?(config.timezone) ->
        "Timezone is invalid."

      true ->
        nil
    end
  end

  @doc """
  Whether this configuration asks for a period at all.

  ## Examples

      iex> FavnView.AssetRunConfig.window_context_requested?(FavnView.AssetRunConfig.default())
      false
  """
  @spec window_context_requested?(map()) :: boolean()
  def window_context_requested?(config),
    do: not blank?(Map.get(config, :value)) or not blank?(Map.get(config, :to))

  @doc """
  Whether this configuration runs a range of periods rather than one.

  An end bound turns one run into a backfill, so the submission path asks this rather
  than reading `:to` itself — a whitespace-only end is not a range, and answering that
  in two places is how one of them comes to disagree.

  ## Examples

      iex> FavnView.AssetRunConfig.range_requested?(%{to: "   "})
      false

      iex> FavnView.AssetRunConfig.range_requested?(%{to: "2026-07-03"})
      true
  """
  @spec range_requested?(map()) :: boolean()
  def range_requested?(config), do: not blank?(Map.get(config, :to))

  defp from_backend(config) do
    %{
      dependencies: config |> Map.get(:dependencies, :all) |> dependencies_value(),
      refresh: config |> Map.get(:refresh, :auto) |> refresh_value(),
      source: config |> Map.get(:source) |> source_value(),
      kind: config |> Map.get(:kind) |> kind_value(),
      value: config |> Map.get(:value, "") |> to_string(),
      to: config |> Map.get(:to, "") |> to_string(),
      timezone: config |> Map.get(:timezone, "Etc/UTC") |> to_string()
    }
  end

  defp dependencies_value(value) when is_atom(value), do: Atom.to_string(value)
  defp dependencies_value(value) when is_binary(value), do: value
  defp dependencies_value(_value), do: "all"

  defp kind_value(value) when value in [:hour, :day, :month, :year], do: Atom.to_string(value)
  defp kind_value(value) when value in ["hour", "day", "month", "year"], do: value
  defp kind_value(_value), do: ""

  defp refresh_value(value) when value in [:auto, "auto"], do: "auto"
  defp refresh_value(value) when value in [:missing, "missing"], do: "missing"

  defp refresh_value(value) when value in [:force, :force_all, "force", "force_all"],
    do: "force_all"

  defp refresh_value(value) when value in [:force_selected, "force_selected"],
    do: "force_selected"

  defp refresh_value(value) when value in [:force_selected_upstream, "force_selected_upstream"],
    do: "force_selected_upstream"

  defp refresh_value(_value), do: "auto"

  defp source_value(:refresh_timeline), do: "refresh_timeline"
  defp source_value(:data_coverage_timeline), do: "data_coverage_timeline"
  defp source_value("refresh_timeline"), do: "refresh_timeline"
  defp source_value("data_coverage_timeline"), do: "data_coverage_timeline"
  defp source_value(_source), do: nil

  defp blank?(value), do: not is_binary(value) or String.trim(value) == ""
  defp valid_timezone?(value) when is_binary(value), do: String.match?(value, @timezone_pattern)
  defp valid_timezone?(_value), do: false
end
