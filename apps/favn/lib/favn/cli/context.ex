defmodule Favn.CLI.Context do
  @moduledoc false

  @spec resolve(keyword()) ::
          {:ok, String.t(), %{service_token: String.t()}, map()} | {:error, term()}
  def resolve(opts) when is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())

    case explicit_context(opts, env) do
      {:ok, _url, _credentials, _context} = result -> result
      :missing -> local_context(opts)
      {:error, _reason} = error -> error
    end
  end

  defp explicit_context(opts, env) do
    url = Keyword.get(opts, :orchestrator_url) || Map.get(env, "FAVN_ORCHESTRATOR_URL")
    token = Keyword.get(opts, :service_token) || Map.get(env, "FAVN_ORCHESTRATOR_SERVICE_TOKEN")
    workspace_id = Keyword.get(opts, :workspace_id) || Map.get(env, "FAVN_WORKSPACE_ID")

    case {present(url), present(token), present(workspace_id)} do
      {nil, nil, nil} ->
        :missing

      {url, token, workspace_id}
      when is_binary(url) and is_binary(token) and is_binary(workspace_id) ->
        {:ok, url, %{service_token: token}, %{"workspace_id" => workspace_id}}

      _incomplete ->
        {:error,
         {:incomplete_cli_environment,
          ~w(FAVN_ORCHESTRATOR_URL FAVN_ORCHESTRATOR_SERVICE_TOKEN FAVN_WORKSPACE_ID)}}
    end
  end

  defp local_context(opts) do
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()

    with {:ok, local} <- FavnLocal.Locator.local_client_options(root_dir) do
      {:ok, local[:orchestrator_url], %{service_token: local[:service_token]},
       %{"workspace_id" => local[:workspace_id]}}
    end
  end

  defp present(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      value -> value
    end
  end

  defp present(_value), do: nil
end
