defmodule CrmDemo.Warehouse.Mart.Sales do
  @moduledoc "Sales reporting models."

  use Favn.Namespace

  relation(schema: "sales")

  meta(tags: [:sales])
end
