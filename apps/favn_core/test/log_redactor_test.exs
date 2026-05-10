defmodule Favn.Log.RedactorTest do
  use ExUnit.Case, async: true

  alias Favn.Log.Entry
  alias Favn.Log.Redactor

  test "preserves SQL and multiline messages by default" do
    message = "SELECT *\nFROM users\nWHERE email = 'a@example.com'"
    entry = %Entry{message: message, metadata: %{query: message, rows: 2}}

    assert {redacted, false} = Redactor.redact(entry)
    assert redacted.message == message
    assert redacted.metadata.query == message
    assert String.contains?(redacted.message, "\nFROM users\n")
  end

  test "redacts conservative metadata keys by default" do
    entry = %Entry{
      message: "connected to warehouse",
      metadata: %{password: "secret", nested: %{"api_key" => "key", status: "ok"}}
    }

    assert {redacted, true} = Redactor.redact(entry)
    assert redacted.message == "connected to warehouse"
    assert redacted.metadata.password == "[REDACTED]"
    assert redacted.metadata.nested["api_key"] == "[REDACTED]"
    assert redacted.metadata.nested.status == "ok"
  end

  test "redacts explicit configured values in message and metadata" do
    entry = %Entry{
      message: "failed with token abc123\nretrying",
      metadata: %{detail: "token abc123"}
    }

    assert {redacted, true} = Redactor.redact(entry, redact_values: ["abc123"])
    assert redacted.message == "failed with token [REDACTED]\nretrying"
    assert redacted.metadata.detail == "token [REDACTED]"
  end

  test "supports explicit patterns and none mode" do
    entry = %Entry{message: "session id=s-123", metadata: %{private_id: "s-123"}}

    assert {redacted, true} =
             Redactor.redact(entry,
               redact_key_patterns: [~r/private/],
               redact_patterns: [~r/id=s-\d+/]
             )

    assert redacted.message == "session [REDACTED]"
    assert redacted.metadata.private_id == "[REDACTED]"

    assert {unchanged, false} = Redactor.redact(entry, :none)
    assert unchanged == entry
  end
end
