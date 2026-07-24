defmodule FavnView.OperatorSessionController do
  @moduledoc false

  use FavnView, :controller

  alias FavnView.Auth

  def new(conn, params) do
    return_to = Auth.safe_return_to(params["return_to"])

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

  def create(conn, %{"operator" => operator_params}) do
    workspace_id = operator_params |> Map.get("workspace_id", "") |> String.trim()
    username = operator_params |> Map.get("username", "") |> String.trim()
    password = Map.get(operator_params, "password", "")
    return_to = Auth.safe_return_to(Map.get(operator_params, "return_to"))

    case FavnOrchestrator.operator_password_login(workspace_id, username, password,
           remote_identity: remote_ip(conn)
         ) do
      {:ok, session, _actor} ->
        Auth.log_in_operator(conn, workspace_id, session, return_to)

      {:error, :invalid_credentials} ->
        conn
        |> put_status(:unauthorized)
        |> put_flash(:error, "Invalid username or password")
        |> render_login(workspace_id, username, return_to)
    end
  end

  def create(conn, _params) do
    conn
    |> put_status(:unauthorized)
    |> put_flash(:error, "Invalid username or password")
    |> render_login("", "", nil)
  end

  def delete(conn, _params), do: Auth.log_out_operator(conn)

  defp source_development_login(conn, return_to) do
    case Application.get_env(:favn_view, :source_development_passwordless_login) do
      %{workspace_id: workspace_id, username: username, capability: capability}
      when is_binary(workspace_id) and workspace_id != "" and is_binary(username) and
             username != "" and is_binary(capability) and capability != "" ->
        case FavnOrchestrator.trusted_local_development_login(
               workspace_id,
               username,
               capability
             ) do
          {:ok, session, _actor} ->
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

  defp remote_ip(conn), do: conn.remote_ip |> :inet.ntoa() |> to_string()
end
