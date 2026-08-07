defmodule FavnStoragePostgres.Bootstrap.PolicyFindingsTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.Bootstrap.Database
  alias FavnStoragePostgres.Bootstrap.Scram
  alias FavnStoragePostgres.RuntimePrivileges

  test "SCRAM verifier is deterministic for a supplied salt and omits plaintext" do
    verifier = Scram.verifier("plaintext-password-canary", 4096, :binary.copy(<<7>>, 16))

    assert verifier ==
             "SCRAM-SHA-256$4096:BwcHBwcHBwcHBwcHBwcHBw==$8cTwsleaO3kCs71l4/FDw9fYumnHonmpc4V4R3/RXBU=:v8zZnhGVFq8Hjor/N+Mbro6K2vMoR8JtZf9bp6j9JII="

    refute verifier =~ "plaintext-password-canary"
  end

  test "SCRAM verifier uses the server-provided iteration policy" do
    verifier = Scram.verifier("plaintext-password-canary", 8192, :binary.copy(<<7>>, 16))

    assert verifier ==
             "SCRAM-SHA-256$8192:BwcHBwcHBwcHBwcHBwcHBw==$RZy4bQ60NAxG4IVlFNfALh/36tHBn/KBoWeIM7rje8U=:aMCGoT0Pomt2Na6uvlfi2U49lugOjaeY3n4NTHLGrtU="
  end

  test "password boundary accepts only SASLprep-stable graphic ASCII" do
    assert Scram.password_supported?("random-ASCII_123!~")

    refute Scram.password_supported?("")
    refute Scram.password_supported?("contains space")
    refute Scram.password_supported?("unicode-påssword")
    refute Scram.password_supported?("line\nbreak")
  end

  test "database policy exposes each unsafe fact with a stable finding" do
    findings =
      Database.findings(
        %{
          database_owner: "favn_migrator",
          migrator_database_connect_only?: true,
          runtime_database_connect_only?: false,
          public_database_connect?: true,
          public_database_create?: false,
          public_database_temporary?: true,
          migrator_database_create?: false,
          migrator_database_temporary?: false,
          runtime_database_create?: false,
          runtime_database_temporary?: false,
          public_schema_create?: false,
          control_schema: %{exists?: true, owner: "favn_migrator"},
          migrator_owns_outside_control?: false,
          control_objects_owned_by_others?: false,
          runtime_owns_objects?: false
        },
        "favn_migrator",
        "favn_runtime"
      )

    assert Enum.map(findings, & &1.code) == [
             :database_owned_by_normal_role,
             :runtime_database_acl_not_exact,
             :public_database_connect,
             :public_database_temporary
           ]
  end

  test "runtime policy distinguishes missing grants from excess authority" do
    diagnostics = %{
      role: "favn_runtime",
      connect?: true,
      superuser?: false,
      create_database?: false,
      create_role?: false,
      inherit?: false,
      replication?: false,
      bypass_rls?: false,
      database_create?: false,
      database_temporary?: false,
      public_schema_create?: false,
      control_schema_usage?: true,
      control_schema_create?: false,
      member_of_roles?: false,
      table_select?: true,
      table_dml?: false,
      schema_migrations_dml?: false,
      unsafe_table_privileges?: false,
      sequence_use?: true,
      sequence_update?: true,
      default_table_dml?: true,
      default_sequence_use?: true,
      unsafe_default_privileges?: false,
      grant_options?: false,
      public_default_privileges?: false,
      public_object_privileges?: false
    }

    assert RuntimePrivileges.findings(diagnostics) == [
             %{
               code: :runtime_table_dml_missing,
               stage: :runtime_grants,
               details: %{expected_role: "favn_runtime", category: :missing}
             },
             %{
               code: :runtime_sequence_update,
               stage: :runtime_grants,
               details: %{expected_role: "favn_runtime", category: :unsafe}
             }
           ]
  end
end
