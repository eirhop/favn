defmodule FavnTestSupport.WarehouseConnection do
  @moduledoc false

  def definition do
    struct!(Favn.Connection.Definition,
      name: :warehouse,
      adapter: FavnTestSupport.TargetAdapter,
      config_schema: []
    )
  end
end

defmodule FavnTestSupport.TargetAdapter do
  @moduledoc false
end
