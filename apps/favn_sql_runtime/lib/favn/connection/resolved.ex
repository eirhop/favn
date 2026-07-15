defmodule Favn.Connection.Resolved do
  @moduledoc """
  Validated, merged runtime connection payload stored in the registry.
  """

  @enforce_keys [:name, :adapter, :module, :config]
  defstruct [
    :name,
    :adapter,
    :module,
    :config,
    required_keys: [],
    secret_fields: [],
    secret_paths: [],
    schema_keys: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          adapter: module(),
          module: module(),
          config: map(),
          required_keys: [atom()],
          secret_fields: [atom()],
          secret_paths: [[atom() | String.t() | non_neg_integer()]],
          schema_keys: [atom()],
          metadata: map()
        }
end
