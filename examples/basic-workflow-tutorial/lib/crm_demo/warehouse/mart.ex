defmodule CrmDemo.Warehouse.Mart do
  @moduledoc """
  Analytics-facing models shaped for a specific question.

  Marts are allowed to aggregate, denormalize, and pick opinionated names. They
  read Core, so a mart never has to know how many source systems fed it.
  """

  use Favn.Namespace

  relation(catalog: "mart")
  meta(tags: [:mart])
end
