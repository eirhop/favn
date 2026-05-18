defmodule Favn.SQL.ConcurrencyPolicies do
  @moduledoc false

  alias Favn.SQL.ConcurrencyPolicy

  @enforce_keys [:default]
  defstruct [:default, catalog: %{}]

  @type target :: :default | {:catalog, binary()}
  @type t :: %__MODULE__{
          default: ConcurrencyPolicy.t() | nil,
          catalog: %{optional(binary()) => ConcurrencyPolicy.t()}
        }

  @spec new(ConcurrencyPolicy.t() | nil, [ConcurrencyPolicy.t()]) :: t()
  def new(default, policies \\ []) do
    catalog =
      policies
      |> Enum.filter(&match?(%ConcurrencyPolicy{target: {:catalog, catalog}} when is_binary(catalog), &1))
      |> Map.new(fn %ConcurrencyPolicy{target: {:catalog, catalog}} = policy -> {catalog, policy} end)

    %__MODULE__{default: default, catalog: catalog}
  end

  @spec catalog_policy(t(), binary() | nil) :: ConcurrencyPolicy.t() | nil
  def catalog_policy(%__MODULE__{catalog: catalog}, name) when is_binary(name) do
    Map.get(catalog, name)
  end

  def catalog_policy(%__MODULE__{}, _name), do: nil
end
