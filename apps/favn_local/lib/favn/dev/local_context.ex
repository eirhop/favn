defmodule Favn.Dev.LocalContext do
  @moduledoc false

  @actor_id "local-dev-cli"
  @session_id "local-dev-cli"

  alias Favn.Dev.Config

  @spec credentials() :: %{service_token: String.t()}
  def credentials, do: %{service_token: ""}

  @spec session_context() :: map()
  def session_context do
    %{
      "actor_id" => @actor_id,
      "session_id" => @session_id,
      "local_dev_context" => "trusted",
      "workspace_id" => Config.resolve().workspace_id
    }
  end
end
