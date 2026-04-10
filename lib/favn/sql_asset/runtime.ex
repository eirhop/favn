defmodule Favn.SQLAsset.Runtime do
  @moduledoc false

  alias Favn.Run.Context

  @spec run(module(), Context.t()) :: {:error, :sql_asset_runtime_not_implemented}
  def run(_module, %Context{}), do: {:error, :sql_asset_runtime_not_implemented}
end
