defmodule Favn.Connection.Error do
  @moduledoc """
  Normalized connection definition/config validation error.
  """

  @enforce_keys [:type, :message]
  defstruct [:type, :message, :connection, :module, details: %{}]

  @type type ::
          :invalid_connection_modules
          | :invalid_connections_config
           | :invalid_module
           | :invalid_definition
           | :duplicate_name
          | :missing_connection
           | :missing_required
          | :missing_env
          | :unknown_keys
          | :invalid_type
          | :invalid_adapter

  @type t :: %__MODULE__{
          type: type(),
          message: String.t(),
          connection: atom() | nil,
          module: module() | nil,
          details: map()
        }
end

defmodule Favn.Connection.ConfigError do
  @moduledoc """
  Raised when connection boot loading fails with one or more normalized errors.
  """

  defexception [:message, errors: []]

  @impl true
  def exception(opts) do
    errors = Keyword.get(opts, :errors, [])

    message =
      (["connection configuration is invalid"] ++
         Enum.map(errors, fn error -> "- #{error.message}" end))
      |> Enum.join("\n")

    %__MODULE__{message: message, errors: errors}
  end
end
