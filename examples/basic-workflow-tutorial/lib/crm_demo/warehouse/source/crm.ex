defmodule CrmDemo.Warehouse.Source.Crm do
  @moduledoc """
  Shared configuration for the CRM source system.

  Every relation below this namespace resolves its input the same way, so the
  resolver is declared once here. `CrmDemo.Warehouse.Source.Crm.Inputs` picks
  one completed landing manifest and turns it into SQL bind values.
  """

  use Favn.Namespace

  relation(schema: "crm")

  runtime_inputs(CrmDemo.Warehouse.Source.Crm.Inputs)
  meta(tags: [:crm])
end
