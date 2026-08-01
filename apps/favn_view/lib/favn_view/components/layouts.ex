defmodule FavnView.Layouts do
  @moduledoc """
  Root layout for the Favn operator UI.

  The root layout owns only the HTML document: `<head>`, theme attribute, and
  asset tags. Every visible frame — navigation, header, mode rail, flash — is
  owned by `FavnView.Components.AppShell`, so a LiveView renders one page
  component and nothing else.
  """
  use FavnView, :html

  embed_templates "layouts/*"

  defp operator_command_scope(%FavnView.Auth.Scope{
         workspace_id: workspace_id,
         actor: %{id: actor_id}
       }) do
    Enum.join([workspace_id, actor_id], ":")
  end

  defp operator_command_scope(_scope), do: nil
end
