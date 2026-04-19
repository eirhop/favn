defmodule FavnOrchestrator.API.Config do
  @moduledoc false

  @spec validate() :: :ok | {:error, term()}
  def validate do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      tokens = Application.get_env(:favn_orchestrator, :api_service_tokens, [])

      case Enum.filter(tokens, &(is_binary(&1) and &1 != "")) do
        [] -> {:error, {:invalid_api_config, :missing_service_tokens}}
        _ -> :ok
      end
    else
      :ok
    end
  end
end
