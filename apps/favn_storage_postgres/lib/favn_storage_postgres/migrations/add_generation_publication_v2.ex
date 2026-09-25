defmodule FavnStoragePostgres.Migrations.AddGenerationPublicationV2 do
  use Ecto.Migration
  @prefix "favn_control"

  def up do
    alter table(:runner_tasks, prefix: @prefix) do
      add(:generation_precondition, :map)
    end

    create(
      constraint(:runner_tasks, :runner_tasks_generation_precondition_bounded,
        prefix: @prefix,
        check:
          "generation_precondition IS NULL OR (jsonb_typeof(generation_precondition) = 'object' AND octet_length(generation_precondition::text) <= 32768)"
      )
    )
  end

  def down,
    do:
      raise("Atomic generation publication requires matched fresh runner and orchestrator state")
end
