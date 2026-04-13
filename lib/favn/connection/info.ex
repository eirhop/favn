defmodule Favn.Connection.Info do
  @moduledoc """
  Public redacted connection inspection payload returned by `Favn` APIs.

  This shape is intentionally stable and safe for operator-facing inspection.
  Runtime-only details remain on `%Favn.Connection.Resolved{}`.
  """

  @enforce_keys [:name, :adapter, :module, :config]
  defstruct [
    :name,
    :adapter,
    :module,
    :config,
    required_keys: [],
    secret_fields: [],
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
          schema_keys: [atom()],
          metadata: map()
        }
end
