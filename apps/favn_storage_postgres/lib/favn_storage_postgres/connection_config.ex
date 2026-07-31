defmodule FavnStoragePostgres.ConnectionConfig do
  @moduledoc false

  @enforce_keys [:authentication, :repo_options, :notification_options]
  defstruct [:authentication, :repo_options, :notification_options]

  @type authentication :: :password | {:dynamic, module(), keyword()}

  @type t :: %__MODULE__{
          authentication: authentication(),
          repo_options: keyword(),
          notification_options: keyword()
        }
end
