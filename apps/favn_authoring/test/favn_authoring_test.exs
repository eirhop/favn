defmodule FavnAuthoringTest do
  use ExUnit.Case, async: false

  defmodule ConfiguredAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule DirectAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  setup do
    previous_assets = Application.get_env(:favn, :asset_modules)

    on_exit(fn ->
      restore_env(:asset_modules, previous_assets)
    end)

    :ok
  end

  test "list_assets/0 uses configured asset modules" do
    Application.put_env(:favn, :asset_modules, [ConfiguredAsset])

    assert {:ok, [%Favn.Asset{ref: {ConfiguredAsset, :asset}}]} = FavnAuthoring.list_assets()
  end

  test "list_assets/1 compiles an omitted module directly" do
    Application.put_env(:favn, :asset_modules, [ConfiguredAsset])

    assert {:ok, [%Favn.Asset{ref: {DirectAsset, :asset}}]} =
             FavnAuthoring.list_assets(DirectAsset)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
