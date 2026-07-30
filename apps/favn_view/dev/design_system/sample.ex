defmodule FavnView.Dev.DesignSystem.Sample do
  @moduledoc """
  Invents a plausible value for a declared attr.

  This is what lets `mode=defaults` and `mode=matrix` render a component that
  has no curated example: everything optional comes from the component's own
  `default:`, and everything required comes from here.

  The rules are deliberately shallow. A component whose required attr is a
  domain-shaped map cannot be guessed, and this module says so by returning
  `:unavailable` rather than inventing a map that happens to have the right
  keys today. `FavnView.Dev.DesignSystem.Generated` turns that into a visible
  "needs a curated example" note on the index.
  """

  alias FavnView.Dev.DesignSystem.Entry

  @unavailable :unavailable

  # A required attr whose type is `:string` but whose values are a closed
  # vocabulary the type cannot express. Keep this table small: it exists for the
  # cases where a generic sample would render something invalid rather than
  # something plain.
  @overrides %{
    {FavnView.UI.Icon, :icon, :name} => "hero-sparkles"
  }

  @doc """
  A sample value for an attr, or `:unavailable` when none can be invented.

  Resolution order: a per-component override, then the attr's declared `values`
  (the first is valid by definition), then its name, then its type.
  """
  @spec for_attr(Entry.t(), map()) :: {:ok, term()} | :unavailable
  def for_attr(%Entry{module: module, function: function}, %{name: name, opts: opts} = attr) do
    cond do
      value = Map.get(@overrides, {module, function, name}) -> {:ok, value}
      values = Keyword.get(opts, :values) -> {:ok, hd(values)}
      true -> by_name_or_type(attr)
    end
  end

  defp by_name_or_type(%{name: name, type: type}) do
    case by_name(name) do
      @unavailable -> by_type(type)
      value -> {:ok, value}
    end
  end

  defp by_name(:label), do: "Label"
  defp by_name(:title), do: "Title"
  defp by_name(:subtitle), do: "Subtitle"
  defp by_name(:description), do: "A one-line description."
  defp by_name(:hint), do: "Hint"
  defp by_name(:name), do: "field"
  defp by_name(:value), do: "value"
  defp by_name(:tone), do: :info
  defp by_name(:status), do: :ok
  defp by_name(:icon), do: "hero-sparkles"
  defp by_name(:id), do: "design-system-example"
  defp by_name(:options), do: [{"Option", "option"}]
  defp by_name(:facts), do: [%{label: "Label", value: "Value"}]
  defp by_name(:rows), do: []
  defp by_name(:items), do: []
  defp by_name(:modes), do: [%{id: :one, label: "One", icon: "hero-square-2-stack"}]
  defp by_name(:on_change), do: "noop"
  defp by_name(_name), do: @unavailable

  defp by_type(:string), do: {:ok, "Example"}
  defp by_type(:integer), do: {:ok, 1}
  defp by_type(:float), do: {:ok, 1.0}
  defp by_type(:boolean), do: {:ok, false}
  defp by_type(:list), do: {:ok, []}
  defp by_type(_type), do: @unavailable

  @doc """
  Sample inner content for a component whose `inner_block` is required.
  """
  @spec inner_block() :: String.t()
  def inner_block, do: "Example content"
end
