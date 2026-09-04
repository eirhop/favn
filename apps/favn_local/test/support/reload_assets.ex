defmodule FavnLocal.TestSupport.ReloadAssets do
  @moduledoc false

  alias FavnLocal.TestSupport.DrainAsset

  def __favn_assets__ do
    doc = "FAVN_RELOAD_FIXTURE_FILE" |> System.fetch_env!() |> File.read!()
    Enum.map(DrainAsset.__favn_assets__(), &%{&1 | doc: doc})
  end
end
