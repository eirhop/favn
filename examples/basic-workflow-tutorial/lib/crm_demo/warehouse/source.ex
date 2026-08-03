defmodule CrmDemo.Warehouse.Source do
  @moduledoc """
  Typed relations that mirror what each source system actually sent.

  Source assets read one completed landing manifest's file list and nothing
  else. They rename and cast fields, and they publish an explicit contract.
  They do not join across systems and they do not apply business rules - that
  is Core's job.
  """

  use Favn.Namespace

  relation(catalog: "source")
  meta(tags: [:source])
end
