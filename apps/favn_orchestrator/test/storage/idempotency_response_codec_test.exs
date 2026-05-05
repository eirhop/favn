defmodule FavnOrchestrator.Storage.IdempotencyResponseCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.IdempotencyResponseCodec

  defmodule SampleStruct do
    defstruct [:name, :secret]
  end

  test "encodes operation-specific run response DTO envelope" do
    at = ~U[2026-05-05 12:00:00Z]

    body = %{
      run: %{
        id: "run_codec",
        status: :pending,
        nested: %{atom_key: {:ok, at}},
        struct: %SampleStruct{name: "visible", secret: "hidden-secret"},
        token: "raw-token"
      },
      ignored: "not persisted"
    }

    assert {:ok, payload} = IdempotencyResponseCodec.encode("run.submit", body)
    assert {:ok, decoded_payload} = Jason.decode(payload)

    assert decoded_payload["format"] == "favn.idempotency_response.storage.v1"
    assert decoded_payload["schema_version"] == 1
    assert decoded_payload["operation"] == "run.submit"
    assert decoded_payload["response_schema"] == "favn.command.run_submit.response.v1"
    refute Map.has_key?(decoded_payload["body"], "ignored")

    raw = payload
    refute raw =~ "__type__"
    refute raw =~ "__struct__"
    refute raw =~ "raw-token"
    refute raw =~ "hidden-secret"

    assert {:ok, body} = IdempotencyResponseCodec.decode(payload)

    assert body == %{
             "run" => %{
               "id" => "run_codec",
               "nested" => %{"atom_key" => ["ok", "2026-05-05T12:00:00Z"]},
               "status" => "pending",
               "struct" => %{"name" => "visible", "secret" => "[REDACTED]"},
               "token" => "[REDACTED]"
             }
           }
  end

  test "encodes command success response DTOs with allow-listed fields" do
    assert {:ok, payload} =
             IdempotencyResponseCodec.encode("manifest.activate", %{
               activated: true,
               manifest_version_id: "mv_1",
               session_token: "secret"
             })

    assert {:ok, body} = IdempotencyResponseCodec.decode(payload)
    assert body == %{"activated" => true, "manifest_version_id" => "mv_1"}
    refute payload =~ "session_token"
    refute payload =~ "secret"

    assert {:ok, payload} =
             IdempotencyResponseCodec.encode("run.cancel", %{
               cancelled: true,
               run_id: "run_1",
               authorization: "Bearer secret"
             })

    assert {:ok, body} = IdempotencyResponseCodec.decode(payload)
    assert body == %{"cancelled" => true, "run_id" => "run_1"}
    refute payload =~ "Bearer secret"
  end

  test "encodes shared error response DTO for known operations" do
    assert {:ok, payload} =
             IdempotencyResponseCodec.encode("run.submit", %{
               code: :validation_failed,
               message: "Invalid request",
               details: %{reason: {:bad, :target}, password: "secret"}
             })

    assert {:ok, decoded_payload} = Jason.decode(payload)
    assert decoded_payload["response_schema"] == "favn.command.error.response.v1"

    assert {:ok, body} = IdempotencyResponseCodec.decode(payload)

    assert body == %{
             "code" => "validation_failed",
             "message" => "Invalid request",
             "details" => %{
               "password" => "[REDACTED]",
               "reason" => %{"module" => "bad", "name" => "target"}
             }
           }
  end

  test "rejects unknown operations and mismatched schemas" do
    assert {:error, {:unsupported_idempotency_operation, "unknown.command"}} =
             IdempotencyResponseCodec.encode("unknown.command", %{})

    payload =
      Jason.encode!(%{
        "format" => "favn.idempotency_response.storage.v1",
        "schema_version" => 1,
        "operation" => "run.submit",
        "response_schema" => "favn.command.run_cancel.response.v1",
        "body" => %{}
      })

    assert {:error,
            {:idempotency_response_schema_mismatch, "run.submit",
             "favn.command.run_cancel.response.v1", "favn.command.run_submit.response.v1"}} =
             IdempotencyResponseCodec.decode(payload)
  end

  test "rejects invalid envelope format and version" do
    invalid_format = Jason.encode!(%{"format" => "payload_codec", "schema_version" => 1})

    assert {:error, {:invalid_idempotency_response_format, "payload_codec"}} =
             IdempotencyResponseCodec.decode(invalid_format)

    invalid_version =
      Jason.encode!(%{
        "format" => "favn.idempotency_response.storage.v1",
        "schema_version" => 2
      })

    assert {:error, {:unsupported_idempotency_response_schema_version, 2}} =
             IdempotencyResponseCodec.decode(invalid_version)
  end
end
