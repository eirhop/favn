defmodule Mix.Tasks.Favn.Postgres.VerifySchema do
  @moduledoc "Verifies the exact PostgreSQL Storage V2 release contract."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Verifies PostgreSQL Storage V2 readiness"

  @impl true
  def run([]) do
    Mix.Task.run("app.config")
    Release.verify_schema() |> ReleaseHelpers.report("PostgreSQL Storage V2 schema is ready")
  end

  def run(_args), do: Mix.raise("usage: mix favn.postgres.verify_schema")
end

defmodule Mix.Tasks.Favn.Postgres.RuntimeInputKeyInventory do
  @moduledoc "Lists runtime-input key versions and reference counts without key material."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Lists runtime-input key-version inventory"

  @impl true
  def run([]) do
    Mix.Task.run("app.config")

    Release.runtime_input_key_inventory()
    |> ReleaseHelpers.report("Runtime-input key inventory")
  end

  def run(_args), do: Mix.raise("usage: mix favn.postgres.runtime_input_key_inventory")
end

defmodule Mix.Tasks.Favn.Postgres.PreflightUpgrade do
  @moduledoc "Checks for active historical manifests before a control-plane upgrade."

  use Mix.Task

  alias FavnStoragePostgres.Release
  alias Mix.Tasks.Favn.Postgres.ReleaseHelpers

  @shortdoc "Checks PostgreSQL upgrade blockers"

  @impl true
  def run([]) do
    Mix.Task.run("app.config")
    Release.preflight_upgrade() |> ReleaseHelpers.report("PostgreSQL upgrade preflight passed")
  end

  def run(_args), do: Mix.raise("usage: mix favn.postgres.preflight_upgrade")
end
