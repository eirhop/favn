defmodule Mix.Tasks.Favn.Admin.Recover do
  use Mix.Task

  @shortdoc "Rotates an administrator password and revokes every session"

  @moduledoc """
  Performs explicit break-glass recovery for an existing administrator.

      mix favn.admin.recover --username USERNAME
      mix favn.admin.recover --username USERNAME \
        --password-file /run/secrets/favn_admin_password

  Recovery activates the global actor and revokes all sessions. It deliberately
  does not alter workspace memberships. Password input follows the same
  stdin/protected-file contract as `mix favn.admin.bootstrap`.
  """

  alias Favn.CLI.AdminSecret
  alias FavnStoragePostgres.Release

  @switches [username: :string, password_file: :string]

  @impl Mix.Task
  def run(args) do
    with {:ok, input, secret_opts} <- parse_args(args),
         {:ok, password} <- AdminSecret.read(secret_opts, "Replacement administrator password"),
         {:ok, result} <- Release.admin_recover(Map.put(input, :password, password)) do
      Mix.shell().info("Administrator recovered: #{result.actor_id}")
      Mix.shell().info("All existing sessions were revoked.")
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

      true ->
        {:ok, %{username: username}, Keyword.take(opts, [:password_file])}
    end
  end

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

  defp format_failure(failure) do
    code = Map.get(failure, :code, :admin_recovery_failed)
    "administrator recovery failed (#{code})"
  end

  defp usage do
    "mix favn.admin.recover --username USERNAME [--password-file PATH]"
  end
end
