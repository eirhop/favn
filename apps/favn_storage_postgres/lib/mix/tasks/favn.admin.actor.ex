defmodule Mix.Tasks.Favn.Admin.Actor do
  use Mix.Task

  @shortdoc "Enables or disables one global operator actor"

  @moduledoc """
  Changes one exact global actor from an explicit trusted host command.

      mix favn.admin.actor --username USERNAME --status disabled
      mix favn.admin.actor --username USERNAME --status active

  Disabling an actor revokes all sessions across every workspace. Workspace
  membership changes remain separate and workspace-scoped.
  """

  alias FavnStoragePostgres.Release

  @switches [username: :string, status: :string]

  @impl Mix.Task
  def run(args) do
    with {:ok, input} <- parse_args(args),
         {:ok, result} <- Release.admin_actor_status(input) do
      Mix.shell().info("Actor #{result.actor_id} is #{result.status}.")

      if result.sessions_revoked do
        Mix.shell().info("All existing actor sessions were revoked.")
      end
    else
      {:error, message} when is_binary(message) -> Mix.raise(message)
      {:error, failure} when is_map(failure) -> Mix.raise(format_failure(failure))
    end
  end

  @doc false
  def parse_args(args) when is_list(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    username = Keyword.get(opts, :username)

    cond do
      invalid != [] ->
        {:error, "invalid option; usage: #{usage()}"}

      rest != [] ->
        {:error, "unexpected argument; usage: #{usage()}"}

      not present?(username) ->
        {:error, "--username is required"}

      Keyword.get(opts, :status) not in ["active", "disabled"] ->
        {:error, "--status must be active or disabled"}

      true ->
        {:ok, %{username: username, status: Keyword.fetch!(opts, :status)}}
    end
  end

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

  defp format_failure(failure) do
    code = Map.get(failure, :code, :actor_status_failed)
    "actor status change failed (#{code})"
  end

  defp usage do
    "mix favn.admin.actor --username USERNAME --status active|disabled"
  end
end
