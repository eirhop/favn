defmodule CrmDemo.Warehouse.Source.Crm.Customers do
  @moduledoc """
  CRM reference data: who the customers are.

  Grouping by responsibility rather than piling every relation into one folder
  keeps the shared settings meaningful - these relations are all full refreshes
  with no window.
  """

  use Favn.Namespace

  meta(tags: [:full_refresh])
end
