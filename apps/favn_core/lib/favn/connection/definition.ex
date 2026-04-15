defmodule Favn.Connection.Definition do
  @moduledoc """
  Canonical static connection definition returned by `Favn.Connection` providers.
  """

  @enforce_keys [:name, :adapter, :config_schema]
  defstruct [
    :name,
    :adapter,
    :module,
    :doc,
    config_schema: [],
    metadata: %{}
  ]

  @typedoc """
  Field type contract for runtime connection values.
  """
  @type field_type ::
          :string
          | :atom
          | :boolean
          | :integer
          | :float
          | :path
          | :module
          | {:in, [term()]}
          | {:custom, (term() -> :ok | {:error, term()})}

  @typedoc """
  Field schema entry describing one allowed runtime key.
  """
  @type field :: %{
          required(:key) => atom(),
          optional(:required) => boolean(),
          optional(:default) => term(),
          optional(:secret) => boolean(),
          optional(:type) => field_type()
        }

  @type t :: %__MODULE__{
          name: atom(),
          adapter: module(),
          module: module() | nil,
          config_schema: [field()],
          doc: String.t() | nil,
          metadata: map()
        }
end
