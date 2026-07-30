defmodule FavnView.WorkspaceSessionController do
  @moduledoc false

  use FavnView, :controller

  alias FavnView.Auth

  def create(conn, %{"workspace_id" => workspace_id}) when is_binary(workspace_id) do
    case Auth.switch_operator_workspace(conn, workspace_id) do
      {:ok, conn} -> conn
      {:error, conn} -> conn
    end
  end

  def create(conn, _params) do
    conn
    |> put_flash(:error, "Could not switch workspace")
    |> redirect(to: "/")
  end
end
