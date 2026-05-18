defmodule FavnView.OperatorSessionController do
  @moduledoc false

  use FavnView, :controller

  alias FavnView.Auth

  def new(conn, params) do
    render(conn, :new,
      username: "",
      return_to: Auth.safe_return_to(params["return_to"]),
      page_title: "Operator sign in"
    )
  end

  def create(conn, %{"operator" => operator_params}) do
    username = operator_params |> Map.get("username", "") |> String.trim()
    password = Map.get(operator_params, "password", "")
    return_to = Auth.safe_return_to(Map.get(operator_params, "return_to"))

    case FavnOrchestrator.operator_password_login(username, password,
           remote_identity: remote_ip(conn)
         ) do
      {:ok, session, _actor} ->
        Auth.log_in_operator(conn, session, return_to)

      {:error, :invalid_credentials} ->
        conn
        |> put_status(:unauthorized)
        |> put_flash(:error, "Invalid username or password")
        |> render(:new,
          username: username,
          return_to: return_to,
          page_title: "Operator sign in"
        )
    end
  end

  def create(conn, _params) do
    conn
    |> put_status(:unauthorized)
    |> put_flash(:error, "Invalid username or password")
    |> render(:new, username: "", return_to: nil, page_title: "Operator sign in")
  end

  def delete(conn, _params), do: Auth.log_out_operator(conn)

  defp remote_ip(conn), do: conn.remote_ip |> :inet.ntoa() |> to_string()
end
