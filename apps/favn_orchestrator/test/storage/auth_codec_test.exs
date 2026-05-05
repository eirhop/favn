defmodule FavnOrchestrator.Storage.AuthCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.AuthCodec

  test "encodes roles as explicit DTO strings" do
    assert {:ok, encoded} = AuthCodec.encode_roles([:admin, :viewer, :admin])

    dto = Jason.decode!(encoded)
    assert dto["format"] == "favn.auth.roles.storage.v1"
    assert dto["schema_version"] == 1
    assert dto["roles"] == ["admin", "viewer"]
    refute encoded =~ "__type__"
    refute encoded =~ "__struct__"

    assert {:ok, [:admin, :viewer]} = AuthCodec.decode_roles(encoded)
  end

  test "rejects malformed and unknown role DTOs" do
    unknown_role =
      Jason.encode!(%{
        "format" => "favn.auth.roles.storage.v1",
        "schema_version" => 1,
        "roles" => ["superadmin"]
      })

    assert {:error, {:unknown_auth_role, "superadmin"}} = AuthCodec.decode_roles(unknown_role)

    malformed =
      Jason.encode!(%{
        "format" => "favn.auth.roles.storage.v1",
        "schema_version" => 1,
        "roles" => "admin"
      })

    assert {:error, {:invalid_auth_roles_field, :roles, "admin"}} =
             AuthCodec.decode_roles(malformed)
  end

  test "encodes credentials as explicit password hash DTOs" do
    credential = %{password_hash: "$argon2id$v=19$m=256,t=1,p=1$encoded-salt$encoded-hash"}

    assert {:ok, encoded} = AuthCodec.encode_credential(credential)

    dto = Jason.decode!(encoded)
    assert dto["format"] == "favn.auth.credential.storage.v1"
    assert dto["schema_version"] == 1
    assert dto["credential"]["kind"] == "password_hash"
    assert dto["credential"]["algorithm"] == "argon2id"
    refute encoded =~ "__type__"
    refute encoded =~ "__struct__"

    assert {:ok, ^credential} = AuthCodec.decode_credential(encoded)
  end

  test "rejects unsupported credential DTOs" do
    unsupported =
      Jason.encode!(%{
        "format" => "favn.auth.credential.storage.v1",
        "schema_version" => 1,
        "credential" => %{
          "kind" => "password_hash",
          "algorithm" => "pbkdf2_sha256",
          "password_hash" => "encoded"
        }
      })

    assert {:error, {:unsupported_auth_credential_algorithm, "pbkdf2_sha256"}} =
             AuthCodec.decode_credential(unsupported)

    wrong_format = Jason.encode!(%{"format" => "json-v1", "schema_version" => 1})

    assert {:error, {:invalid_auth_credential_dto, _dto}} =
             AuthCodec.decode_credential(wrong_format)

    mislabeled_argon2i =
      Jason.encode!(%{
        "format" => "favn.auth.credential.storage.v1",
        "schema_version" => 1,
        "credential" => %{
          "kind" => "password_hash",
          "algorithm" => "argon2id",
          "password_hash" => "$argon2i$v=19$m=256,t=1,p=1$encoded-salt$encoded-hash"
        }
      })

    assert {:error, {:invalid_auth_credential_field, :credential, _credential}} =
             AuthCodec.decode_credential(mislabeled_argon2i)
  end

  test "encodes audit entries as bounded JSON-safe DTOs" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    entry = %{
      "id" => "detail-id",
      "action" => "detail-action",
      id: "aud_codec",
      occurred_at: now,
      action: "auth.test",
      actor_id: "act_codec",
      session_id: "ses_codec",
      outcome: "accepted",
      service_identity: "favn_web",
      password: "secret",
      metadata: %{token: "raw-token", count: 1}
    }

    assert {:ok, encoded} = AuthCodec.encode_audit(entry)

    dto = Jason.decode!(encoded)
    assert dto["format"] == "favn.auth.audit.storage.v1"
    assert dto["schema_version"] == 1
    assert dto["details"]["password"] == "[REDACTED]"
    assert dto["details"]["metadata"]["token"] == "[REDACTED]"
    refute Map.has_key?(dto["details"], "id")
    refute Map.has_key?(dto["details"], "action")
    refute encoded =~ "secret"
    refute encoded =~ "raw-token"
    refute encoded =~ "__type__"
    refute encoded =~ "__struct__"

    assert {:ok, decoded} = AuthCodec.decode_audit(encoded)
    assert decoded.id == "aud_codec"
    assert decoded.occurred_at == now
    assert decoded.action == "auth.test"
    assert decoded["password"] == "[REDACTED]"
    refute Map.has_key?(decoded, "id")
    refute Map.has_key?(decoded, "action")
  end

  test "rejects malformed audit DTOs" do
    malformed_timestamp =
      Jason.encode!(%{
        "format" => "favn.auth.audit.storage.v1",
        "schema_version" => 1,
        "id" => "aud_bad",
        "occurred_at" => "not-a-date",
        "details" => %{}
      })

    assert {:error, {:invalid_auth_audit_field, :occurred_at, "not-a-date"}} =
             AuthCodec.decode_audit(malformed_timestamp)

    missing_id =
      Jason.encode!(%{
        "format" => "favn.auth.audit.storage.v1",
        "schema_version" => 1,
        "occurred_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "details" => %{}
      })

    assert {:error, {:invalid_auth_audit_field, "id", nil}} =
             AuthCodec.decode_audit(missing_id)

    empty_id =
      Jason.encode!(%{
        "format" => "favn.auth.audit.storage.v1",
        "schema_version" => 1,
        "id" => "",
        "occurred_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "details" => %{}
      })

    assert {:error, {:invalid_auth_audit_field, "id", ""}} = AuthCodec.decode_audit(empty_id)

    non_binary_id =
      Jason.encode!(%{
        "format" => "favn.auth.audit.storage.v1",
        "schema_version" => 1,
        "id" => 123,
        "occurred_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "details" => %{}
      })

    assert {:error, {:invalid_auth_audit_field, "id", 123}} =
             AuthCodec.decode_audit(non_binary_id)

    non_binary_action =
      Jason.encode!(%{
        "format" => "favn.auth.audit.storage.v1",
        "schema_version" => 1,
        "id" => "aud_bad_action",
        "occurred_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "action" => 123,
        "details" => %{}
      })

    assert {:error, {:invalid_auth_audit_field, :action, 123}} =
             AuthCodec.decode_audit(non_binary_action)
  end
end
