defmodule Mix.Tasks.Favn.Admin.PasswordReset do
  use Mix.Task

  @shortdoc "Rotates one global actor password and revokes every session"

  @moduledoc """
  Performs explicit platform recovery for one exact actor.

      mix favn.admin.password_reset --username USERNAME
      mix favn.admin.password_reset --username USERNAME --password-file /run/secrets/favn_actor_password

  The actor's status and memberships are preserved. Password input follows the
  same stdin/protected-file contract as `mix favn.admin.bootstrap`.
  """

  alias FavnStoragePostgres.AdminSecret
  alias FavnStoragePostgres.Release

  @switches [username: :string, password_file: :string]

  @impl Mix.Task
  def run(args) do
    with {:ok, input, secret_opts} <- parse_args(args),
         {:ok, password} <- AdminSecret.read(secret_opts, "Replacement actor password"),
         {:ok, result} <- Release.admin_password_reset(Map.put(input, :password, password)) do
      Mix.shell().info("Actor password reset: #{result.actor_id}")
      Mix.shell().info("All existing actor sessions were revoked.")
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
    code = Map.get(failure, :code, :actor_password_reset_failed)
    "actor password reset failed (#{code})"
  end

  defp usage do
    "mix favn.admin.password_reset --username USERNAME [--password-file PATH]"
  end
end
