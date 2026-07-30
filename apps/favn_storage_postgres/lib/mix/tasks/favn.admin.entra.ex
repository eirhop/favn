defmodule Mix.Tasks.Favn.Admin.Entra do
  use Mix.Task

  @shortdoc "Links or unlinks an Entra object ID for a Favn actor"

  @moduledoc """
  Configures the immutable Entra identity used by Azure Container Apps Easy Auth.

      mix favn.admin.entra --username USERNAME --tenant-id UUID --object-id UUID --action link
      mix favn.admin.entra --username USERNAME --tenant-id UUID --object-id UUID --action unlink

  Email addresses, display names, Entra groups, and Entra roles are not identity
  keys and cannot grant Favn authorization.
  """

  alias FavnStoragePostgres.Release

  @switches [
    username: :string,
    tenant_id: :string,
    object_id: :string,
    action: :string
  ]

  @impl Mix.Task
  def run(args) do
    with {:ok, input} <- parse_args(args),
         {:ok, result} <- Release.admin_entra_identity(input) do
      Mix.shell().info("Entra identity #{result.action} completed for actor #{result.actor_id}.")
    else
      {:error, message} when is_binary(message) -> Mix.raise(message)
      {:error, failure} when is_map(failure) -> Mix.raise(format_failure(failure))
    end
  end

  @doc false
  def parse_args(args) when is_list(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    cond do
      invalid != [] ->
        {:error, "invalid option; usage: #{usage()}"}

      rest != [] ->
        {:error, "unexpected argument; usage: #{usage()}"}

      not present?(opts[:username]) ->
        {:error, "--username is required"}

      not present?(opts[:tenant_id]) ->
        {:error, "--tenant-id is required"}

      not present?(opts[:object_id]) ->
        {:error, "--object-id is required"}

      opts[:action] not in ["link", "unlink"] ->
        {:error, "--action must be link or unlink"}

      true ->
        {:ok,
         %{
           username: opts[:username],
           tenant_id: opts[:tenant_id],
           object_id: opts[:object_id],
           action: opts[:action]
         }}
    end
  end

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

  defp format_failure(failure) do
    code = Map.get(failure, :code, :entra_identity_failed)
    "Entra identity change failed (#{code})"
  end

  defp usage do
    "mix favn.admin.entra --username USERNAME --tenant-id UUID --object-id UUID --action link|unlink"
  end
end
