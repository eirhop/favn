defmodule CrmDemo.Warehouse.Core.Sales do
  @moduledoc "The sales domain: customers, opportunities, and engagement."

  use Favn.Namespace

  relation(schema: "sales")

  meta(tags: [:sales])
end
