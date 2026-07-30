defmodule Mix.Tasks.Favn.Admin.Bootstrap do
  use Mix.Task

  @shortdoc "Creates Favn's first administrator exactly once"

  @moduledoc """
  Creates the first administrator through an explicit, audited release operation.

      mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME
      mix favn.admin.bootstrap --workspace WS_A --workspace WS_B \
        --username USERNAME --display-name "Platform Administrator"
      mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME \
        --password-file /run/secrets/favn_admin_password

  The password is read without echo from stdin unless `--password-file` names a
  regular Unix file whose group/other permission bits are all disabled. A
  password is never accepted as a command-line value or environment variable.
  """

  alias FavnStoragePostgres.AdminSecret
  alias FavnStoragePostgres.Release

  @switches [
    workspace: [:string, :keep],
    username: :string,
    display_name: :string,
    password_file: :string
  ]

  @impl Mix.Task
  def run(args) do
    with {:ok, input, secret_opts} <- parse_args(args),
         {:ok, password} <- AdminSecret.read(secret_opts, "New administrator password"),
         {:ok, result} <- Release.admin_bootstrap(Map.put(input, :password, password)) do
      Mix.shell().info("Administrator created: #{result.actor_id}")
      Mix.shell().info("Workspaces: #{Enum.join(result.workspace_ids, ", ")}")
    else
      {:error, message} when is_binary(message) -> Mix.raise(message)
      {:error, failure} when is_map(failure) -> Mix.raise(format_failure(failure))
    end
  end

  @doc false
  def parse_args(args) when is_list(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)
    workspace_ids = Keyword.get_values(opts, :workspace)
    username = Keyword.get(opts, :username)
    display_name = Keyword.get(opts, :display_name, username)

    cond do
      invalid != [] ->
        {:error, "invalid option; usage: #{usage()}"}

      rest != [] ->
        {:error, "unexpected argument; usage: #{usage()}"}

      workspace_ids == [] ->
        {:error, "at least one --workspace is required"}

      not present?(username) ->
        {:error, "--username is required"}

      not present?(display_name) ->
        {:error, "--display-name must not be blank"}

      true ->
        {:ok,
         %{
           workspace_ids: workspace_ids,
           username: username,
           display_name: display_name
         }, Keyword.take(opts, [:password_file])}
    end
  end

  defp present?(value), do: is_binary(value) and String.trim(value) != ""

  defp format_failure(failure) do
    code = Map.get(failure, :code, :admin_bootstrap_failed)
    "administrator bootstrap failed (#{code})"
  end

  defp usage do
    "mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME [--display-name NAME] [--password-file PATH]"
  end
end
