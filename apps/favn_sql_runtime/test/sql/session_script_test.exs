defmodule FavnSQLRuntime.SQLSessionScriptTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
  alias Favn.SQL.Error
  alias Favn.SQL.SessionScript
  alias Favn.SQL.SessionScript.Step
  alias Favn.SQL.SessionScript.Template

  @moduletag :tmp_dir

  test "plans startup and the exact required resources in deterministic order", %{tmp_dir: dir} do
    startup = write_sql!(dir, "startup.sql", "SET timezone = @timezone;")
    azure = write_sql!(dir, "azure.sql", "INSTALL azure; LOAD azure;")
    landing = write_sql!(dir, "landing.sql", "CREATE SECRET landing (TOKEN @token);")

    resolved =
      resolved(%{
        duckdb: [
          startup: [file: startup, params: [timezone: "UTC"]],
          resources: [
            landing_storage: [file: landing, params: [token: "secret-token"]],
            azure_extension: [file: azure]
          ],
          catalogs: [lake: [resource: :landing_storage, write_concurrency: 2]]
        ]
      })

    assert {:ok, plan} =
             SessionScript.plan(resolved,
               required_catalogs: [:lake],
               required_resources: [:azure_extension]
             )

    assert plan.catalogs == ["lake"]
    assert plan.resources == ["azure_extension", "landing_storage"]

    assert Enum.map(plan.steps, & &1.id) == [
             "startup",
             "resource:azure_extension",
             "resource:landing_storage"
           ]

    assert Enum.at(plan.steps, 0).statement == "SET timezone = 'UTC';"
    assert List.last(plan.steps).statement == "CREATE SECRET landing (TOKEN 'secret-token');"

    assert List.last(plan.steps).safe_statement ==
             "CREATE SECRET landing (TOKEN '[REDACTED]');"
  end

  test "fingerprint changes with file content and parameters", %{tmp_dir: dir} do
    script = write_sql!(dir, "resource.sql", "SET threads = @threads;")
    resolved = resolved(%{duckdb: [resources: [runtime: [file: script, params: [threads: 2]]]]})

    assert {:ok, first} = SessionScript.fingerprint(resolved, required_resources: [:runtime])

    File.write!(script, "SET threads = @threads; -- changed")

    assert {:ok, changed_file} =
             SessionScript.fingerprint(resolved, required_resources: [:runtime])

    refute first == changed_file

    changed_params =
      resolved(%{duckdb: [resources: [runtime: [file: script, params: [threads: 4]]]]})

    assert {:ok, changed_params} =
             SessionScript.fingerprint(changed_params, required_resources: [:runtime])

    refute changed_file == changed_params
  end

  test "omitted catalogs select all configured catalogs while an explicit empty set selects none",
       %{
         tmp_dir: dir
       } do
    startup = write_sql!(dir, "startup.sql", "SET timezone = 'UTC';")
    raw = write_sql!(dir, "raw.sql", "ATTACH ':memory:' AS raw;")

    resolved =
      resolved(%{
        duckdb: [
          startup: [file: startup],
          resources: [raw_catalog: [file: raw]],
          catalogs: [raw: [resource: :raw_catalog]]
        ]
      })

    assert {:ok, all} = SessionScript.plan(resolved, [])
    assert all.catalogs == ["raw"]
    assert all.resources == ["raw_catalog"]

    assert {:ok, startup_only} = SessionScript.plan(resolved, required_catalogs: [])
    assert startup_only.catalogs == []
    assert startup_only.resources == []
    assert Enum.map(startup_only.steps, & &1.id) == ["startup"]
  end

  test "unknown catalogs and resources fail before a script is executed", %{tmp_dir: dir} do
    script = write_sql!(dir, "known.sql", "SELECT 1;")
    resolved = resolved(%{duckdb: [resources: [known: [file: script]]]})

    assert {:error, %{details: %{reason: {:unknown_required_catalogs, ["missing"]}}}} =
             SessionScript.plan(resolved, required_catalogs: [:missing])

    assert {:error, %{details: %{reason: {:unknown_required_resources, ["missing"]}}}} =
             SessionScript.plan(resolved, required_resources: [:missing])
  end

  test "template parameters are values and are ignored in SQL strings and comments" do
    sql = "select @value, '@not_a_param', \"@identifier\" -- @comment\n/* @block */"

    assert {:ok, rendered} = Template.render(sql, %{value: "x'; drop table users; --"})
    assert rendered.statement =~ "'x''; drop table users; --'"
    assert rendered.statement =~ "'@not_a_param'"
    assert rendered.statement =~ "-- @comment"

    assert {:error, {:unused_script_parameters, ["unused"]}} =
             Template.render("select 1", %{unused: 1})

    assert {:error, {:missing_script_parameters, ["missing"]}} =
             Template.render("select @missing", %{})

    assert {:error, {:invalid_script_parameter, "value", :invalid_utf8}} =
             Template.render("select @value", %{value: <<255>>})
  end

  test "all supported secret parameter types produce redaction tokens" do
    assert {:ok, rendered} =
             Template.render(
               "select @token, @pin",
               %{token: "secret-token", pin: 1234},
               MapSet.new(["token", "pin"])
             )

    assert rendered.safe_statement == "select '[REDACTED]', '[REDACTED]'"
    assert "secret-token" in rendered.secret_values
    assert "'secret-token'" in rendered.secret_values
    assert "1234" in rendered.secret_values
  end

  test "step errors expose only bounded safe SQL and redact all secret tokens" do
    step = %Step{
      id: "resource:storage",
      kind: :resource,
      resource: "storage",
      statement: "select 'secret-token', 1234",
      safe_statement: "select '[REDACTED]', '[REDACTED]'",
      content_hash: "content",
      parameter_hash: "params",
      secret_values: ["secret-token", "'secret-token'", "1234"]
    }

    error = %Error{
      type: :execution_error,
      message: "failed near 'secret-token' and 1234",
      details: %{unsafe: "secret-token"},
      cause: "secret-token"
    }

    redacted = SessionScript.redact_step_error(error, step)

    assert redacted.operation == :bootstrap
    assert redacted.message == "failed near '[REDACTED]' and [REDACTED]"
    assert redacted.details.statement == "select '[REDACTED]', '[REDACTED]'"
    assert redacted.cause == nil
    refute inspect(redacted) =~ "secret-token"
  end

  test "invalid structured bootstrap keys and relative files are rejected" do
    assert {:error, %{details: %{reason: {:unknown_config_keys, :duckdb, [:load]}}}} =
             SessionScript.config(resolved(%{duckdb: [load: [:azure]]}))

    assert {:error, %{details: %{reason: {:invalid_script_file, :absolute_path_required}}}} =
             SessionScript.config(resolved(%{duckdb: [startup: [file: "priv/startup.sql"]]}))

    assert {:error, %{details: %{reason: {:invalid_script_file, :priv_path_cannot_escape}}}} =
             SessionScript.config(
               resolved(%{duckdb: [startup: [file: {:priv, :my_app, "../startup.sql"}]]})
             )
  end

  test "atom and string forms of the same config key are rejected" do
    top_level = %{
      "startup" => %{"file" => "/tmp/second.sql"},
      startup: %{file: "/tmp/first.sql"}
    }

    assert {:error, %{details: %{reason: {:duplicate_config_keys, :duckdb, ["startup"]}}}} =
             SessionScript.config(resolved(%{duckdb: top_level}))

    nested = %{startup: %{"file" => "/tmp/second.sql", file: "/tmp/first.sql"}}

    assert {:error,
            %{
              details: %{
                reason: {:duplicate_config_keys, {:script, "startup"}, ["file"]}
              }
            }} = SessionScript.config(resolved(%{duckdb: nested}))
  end

  test "non-UTF-8 SQL files are rejected before rendering", %{tmp_dir: dir} do
    script = write_sql!(dir, "invalid.sql", <<255>>)

    assert {:error, %{details: %{reason: {:script_file_invalid_utf8, "startup"}}}} =
             SessionScript.plan(resolved(%{duckdb: [startup: [file: script]]}), [])
  end

  defp resolved(config) do
    %Resolved{
      name: :warehouse,
      adapter: __MODULE__,
      module: __MODULE__,
      config: config,
      secret_paths: [[:duckdb, :resources, :landing_storage, :params, :token]]
    }
  end

  defp write_sql!(dir, name, sql) do
    path = Path.join(dir, name)
    File.write!(path, sql)
    path
  end
end
