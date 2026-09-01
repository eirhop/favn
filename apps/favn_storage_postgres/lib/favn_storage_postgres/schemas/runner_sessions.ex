defmodule FavnStoragePostgres.Schemas.RunnerSession do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  @schema_prefix "favn_control"
  schema "runner_sessions" do
    field(:session_id, :string, primary_key: true)
    field(:runner_instance_id, :string)
    field(:runner_boot_id, :string)
    field(:session_generation, :integer)
    field(:control_plane_boot_id, :string)
    field(:runner_pool, :string)
    field(:required_runner_release_id, :string)
    field(:beam_node, :string)
    field(:protocol_version, :integer)
    field(:lifecycle_mode, :string)
    field(:registered_at, :utc_datetime_usec)
    field(:ended_at, :utc_datetime_usec)
    field(:end_reason, :string)
    field(:busy_at_exit, :boolean)
    field(:interrupted_task_workspace_id, :string)
    field(:interrupted_task_id, :string)
    field(:inserted_at, :utc_datetime_usec)
    field(:updated_at, :utc_datetime_usec)
  end
end
