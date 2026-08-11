defmodule FavnView.OperatorSessionController do
  @moduledoc false

  use FavnView, :controller

  alias FavnView.Orchestrator
  alias FavnView.Auth
  alias FavnView.Auth.AzureContainerAppsEntra
  alias FavnView.ProductionRuntimeConfig

  @dialyzer {:no_match, create_password_session: 2}

  def new(conn, params) do
    return_to = Auth.safe_return_to(params["return_to"])

    case ProductionRuntimeConfig.auth() do
      %{mode: :azure_container_apps_entra} = config ->
        entra_login(conn, config, return_to)

      %{mode: :password} ->
        password_login_page(conn, return_to)
    end
  end

  def create(conn, %{"operator" => operator_params}) do
    if ProductionRuntimeConfig.auth().mode == :password do
      create_password_session(conn, operator_params)
    else
      access_denied(conn)
    end
  end

  def create(conn, _params) do
    if ProductionRuntimeConfig.auth().mode == :password do
      conn
      |> put_status(:unauthorized)
      |> put_flash(:error, "Invalid username or password")
      |> render_login("", "", nil)
    else
      access_denied(conn)
    end
  end

  def delete(conn, _params), do: Auth.log_out_operator(conn)

  defp password_login_page(conn, return_to) do
    case source_development_login(conn, return_to) do
      {:ok, conn} ->
        conn

      :disabled ->
        render_login(conn, "", "", return_to)

      {:error, :invalid_credentials} ->
        conn
        |> put_flash(:error, "Automatic local sign-in failed")
        |> render_login("", "", return_to)
    end
  end

  defp create_password_session(conn, operator_params) do
    workspace_id = operator_params |> Map.get("workspace_id", "") |> String.trim()
    username = operator_params |> Map.get("username", "") |> String.trim()
    password = Map.get(operator_params, "password", "")
    return_to = Auth.safe_return_to(Map.get(operator_params, "return_to"))

    case Orchestrator.operator_password_login(workspace_id, username, password,
           remote_identity: remote_ip(conn)
         ) do
      {:ok, session, _actor} ->
        Auth.log_in_operator(conn, workspace_id, session, return_to)

      {:error, :invalid_credentials} ->
        conn
        |> put_status(:unauthorized)
        |> put_flash(:error, "Invalid username or password")
        |> render_login(workspace_id, username, return_to)

      {:error, reason}
      when reason in [:orchestrator_unavailable, :orchestrator_outcome_unknown] ->
        conn
        |> put_status(:service_unavailable)
        |> put_flash(:error, "Sign-in service is temporarily unavailable")
        |> render_login(workspace_id, username, return_to)
    end
  end

  defp source_development_login(conn, return_to) do
    case Application.get_env(:favn_view, :source_development_passwordless_login) do
      %{workspace_id: workspace_id, username: username, capability: capability}
      when is_binary(workspace_id) and workspace_id != "" and is_binary(username) and
             username != "" and is_binary(capability) and capability != "" ->
        case Orchestrator.trusted_local_development_login(
               workspace_id,
               username,
               capability
             ) do
          {:ok, session} ->
            {:ok, Auth.log_in_operator(conn, workspace_id, session, return_to)}

          {:error, :trusted_local_development_unavailable} ->
            {:error, :invalid_credentials}
        end

      _disabled ->
        :disabled
    end
  end

  defp render_login(conn, workspace_id, username, return_to) do
    render(conn, :new,
      workspace_id: workspace_id,
      username: username,
      return_to: return_to,
      page_title: "Operator sign in"
    )
  end

  defp entra_login(conn, config, return_to) do
    with {:ok, identity} <- AzureContainerAppsEntra.identity(conn, config.tenant_id),
         {:ok, session, _actor} <-
           operator_external_login(config.workspace_id, identity) do
      Auth.log_in_operator(conn, config.workspace_id, session, return_to)
    else
      {:error, reason}
      when reason in [:orchestrator_unavailable, :orchestrator_outcome_unknown] ->
        service_unavailable(conn)

      _denied ->
        access_denied(conn)
    end
  end

  defp operator_external_login(workspace_id, identity) do
    login_fun =
      Application.get_env(
        :favn_view,
        :operator_external_login_fun,
        &Orchestrator.operator_external_login/2
      )

    login_fun.(workspace_id, identity)
  end

  defp access_denied(conn) do
    conn
    |> put_status(:forbidden)
    |> put_resp_content_type("text/plain")
    |> send_resp(403, "Access denied")
  end

  defp service_unavailable(conn) do
    conn
    |> put_status(:service_unavailable)
    |> put_resp_content_type("text/plain")
    |> send_resp(503, "Sign-in service unavailable")
  end

  defp remote_ip(conn), do: conn.remote_ip |> :inet.ntoa() |> to_string()
end
