defmodule FavnOrchestrator.Operator.WindowSelection do
  @moduledoc """
  Parses operator timeline selections and resolves their asset windows.

  The operator catalogue emits opaque selection ids. Command paths use this
  module to validate those ids against the selected asset's window policy before
  constructing runner input.
  """

  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias Favn.Window.Request
  alias Favn.Window.Spec

  @doc "Parses a data-coverage timeline selection id."
  @spec data_coverage_request(String.t()) :: {:ok, Request.t()} | {:error, term()}
  def data_coverage_request("window:day:" <> date), do: parse(:day, date)
  def data_coverage_request("window:hour:" <> hour), do: parse(:hour, hour)
  def data_coverage_request("window:month:" <> month), do: parse(:month, month)
  def data_coverage_request("window:year:" <> year), do: parse(:year, year)

  def data_coverage_request(window_id), do: {:error, {:invalid_window_id, window_id}}

  @doc "Parses a refresh timeline selection id."
  @spec refresh_request(String.t()) :: {:ok, Request.t()} | {:error, term()}
  def refresh_request("refresh:hour:" <> hour), do: parse(:hour, hour)
  def refresh_request("refresh:day:" <> date), do: parse(:day, date)
  def refresh_request("refresh:month:" <> month), do: parse(:month, month)
  def refresh_request("refresh:year:" <> year), do: parse(:year, year)
  def refresh_request(id), do: {:error, {:invalid_refresh_id, id}}

  @doc "Resolves a parsed request against an asset's window policy."
  @spec resolve(map(), Request.t()) :: {:ok, Anchor.t()} | {:error, term()}
  def resolve(%{window: nil}, %Request{kind: kind}) do
    {:error, {:window_request_without_policy, kind}}
  end

  def resolve(%{window: %Spec{} = spec}, %Request{kind: kind} = request) do
    if spec.kind == kind do
      Request.to_anchor(request, spec.timezone)
    else
      {:error, {:window_kind_mismatch, spec.kind, kind}}
    end
  end

  def resolve(%{window: window} = asset, %Request{} = request) when is_atom(window) do
    with {:ok, spec} <- Spec.new(window) do
      resolve(%{asset | window: spec}, request)
    end
  end

  def resolve(%{window: %{} = window}, %Request{} = request) do
    case {Map.get(window, :kind) || Map.get(window, "kind"),
          Map.get(window, :timezone) || Map.get(window, "timezone")} do
      {kind, timezone} when not is_nil(kind) ->
        with {:ok, normalized_kind} <- normalize_kind(kind),
             {:ok, spec} <- Spec.new(normalized_kind, timezone: timezone || "Etc/UTC") do
          resolve(%{window: spec}, request)
        end

      _other ->
        {:error, :invalid_window_policy}
    end
  end

  @doc "Normalizes a supported window policy kind."
  @spec normalize_kind(term()) :: {:ok, atom()} | {:error, term()}
  def normalize_kind(kind) do
    case Policy.from_value(kind) do
      {:ok, %Policy{kind: normalized_kind}} -> {:ok, normalized_kind}
      {:ok, nil} -> {:error, {:invalid_window_policy_kind, kind}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse(kind, value) do
    case Request.parse("#{kind}:#{value}") do
      {:ok, request} -> {:ok, request}
      {:error, reason} -> {:error, {:invalid_window_id, reason}}
    end
  end
end
