defmodule Favn.SQLAssetInputTest do
  use ExUnit.Case, async: true

  alias Favn.SQLAsset.Input

  defmodule SQLAsset do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "test", schema: "public"]
    use Favn.SQLAsset

    @materialized :table
    query do
      ~SQL"select 1 as id"
    end
  end

  test "returns SQL asset structs unchanged" do
    asset = %Favn.Asset{ref: {__MODULE__, :asset}, type: :sql}

    assert {:ok, ^asset} = Input.normalize(asset)
  end

  test "rejects non-SQL asset structs" do
    asset = %Favn.Asset{ref: {__MODULE__, :asset}, type: :elixir}

    assert {:error, error} = Input.normalize(asset)

    assert %{type: :not_sql_asset, phase: :render, asset_ref: {__MODULE__, :asset}} =
             payload(error)
  end

  test "rejects non-asset SQL refs" do
    assert {:error, error} = Input.normalize({SQLAsset, :other})

    assert %{
             type: :invalid_asset_input,
             phase: :render,
             asset_ref: {SQLAsset, :other},
             details: %{reason: :invalid_sql_asset_ref_name}
           } = payload(error)
  end

  test "rejects invalid input shapes" do
    input = %{unexpected: true}

    assert {:error, error} = Input.normalize(input)

    assert %{
             type: :invalid_asset_input,
             phase: :render,
             asset_ref: nil,
             details: %{input: ^input}
           } = payload(error)
  end

  test "resolves SQL asset modules" do
    assert {:ok, %Favn.Asset{ref: {SQLAsset, :asset}, type: :sql}} = Input.normalize(SQLAsset)

    assert {:ok, %Favn.Asset{ref: {SQLAsset, :asset}, type: :sql}} =
             Input.normalize({SQLAsset, :asset})
  end

  test "preserves unresolved module reason in error details" do
    module = Favn.SQLAssetInputTest.MissingAsset

    assert {:error, error} = Input.normalize(module)

    assert %{
             type: :invalid_asset_input,
             phase: :render,
             asset_ref: {^module, :asset},
             details: %{reason: reason}
           } = payload(error)

    assert reason in [:not_asset_module, :asset_not_found]
  end

  defp payload(%_struct{} = error), do: Map.from_struct(error)
  defp payload(error), do: error
end
