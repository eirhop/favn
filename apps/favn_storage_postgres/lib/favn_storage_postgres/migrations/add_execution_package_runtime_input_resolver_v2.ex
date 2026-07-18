defmodule FavnStoragePostgres.Migrations.AddExecutionPackageRuntimeInputResolverV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:execution_packages, prefix: @prefix) do
      add(:runtime_input_resolver, :text)
    end

    execute("""
    UPDATE #{@prefix}.execution_packages
    SET runtime_input_resolver = payload #>> '{sql_execution,runtime_inputs,module}'
    WHERE payload #> '{sql_execution,runtime_inputs}' IS NOT NULL
    """)

    create(
      constraint(:execution_packages, :execution_packages_runtime_input_resolver_valid,
        prefix: @prefix,
        check:
          "runtime_input_resolver IS NULL OR octet_length(runtime_input_resolver) BETWEEN 1 AND 255"
      )
    )
  end

  def down do
    drop(
      constraint(:execution_packages, :execution_packages_runtime_input_resolver_valid,
        prefix: @prefix
      )
    )

    alter table(:execution_packages, prefix: @prefix) do
      remove(:runtime_input_resolver)
    end
  end
end
