defmodule FavnSQLRuntime.SQLErrorTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.Error

  test "redacts secret-bearing details, causes, and URI userinfo" do
    error = %Error{
      type: :execution_error,
      message: "failed against postgres://user:password@example/db?token=abc",
      details: %{
        password: "secret-password",
        nested: %{metadata: "postgres://user:password@example/db"},
        reason: "access_key=abc123"
      },
      cause: {:error, %{token: "secret-token", message: "credential=raw"}}
    }

    safe = Error.redact(error)

    refute inspect(safe) =~ "secret-password"
    refute inspect(safe) =~ "secret-token"
    refute inspect(safe) =~ "user:password"
    refute inspect(safe) =~ "abc123"
    refute inspect(safe) =~ "credential=raw"

    assert safe.details.password == :redacted
    assert safe.details.nested.metadata == :redacted
    assert safe.message =~ "postgres://redacted@example/db"
    assert safe.message =~ "token=redacted"
  end
end
