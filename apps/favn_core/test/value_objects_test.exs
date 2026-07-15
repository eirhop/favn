defmodule Favn.ValueObjectsTest do
  use ExUnit.Case, async: true

  alias Favn.RuntimeConfig.Bundle
  alias Favn.RuntimeConfig.Ref
  alias Favn.RuntimeConfig.Requirements

  test "builds canonical asset refs" do
    assert Favn.Ref.new(MyApp.Asset, :asset) == {MyApp.Asset, :asset}
  end

  test "normalizes relation refs including aliases" do
    relation = Favn.RelationRef.new!(database: "raw", schema: :sales, table: :orders)

    assert relation.catalog == "raw"
    assert relation.schema == "sales"
    assert relation.name == "orders"
  end

  test "requires a relation name" do
    assert_raise ArgumentError, "relation ref name is required", fn ->
      Favn.RelationRef.new!(name: nil)
    end
  end

  test "validates timezone identifiers" do
    assert Favn.Timezone.valid_identifier?("Etc/UTC")
    assert Favn.Timezone.valid_identifier?("Europe/Oslo")
    refute Favn.Timezone.valid_identifier?("../etc/passwd")
    refute Favn.Timezone.valid_identifier?("/etc/passwd")
    refute Favn.Timezone.valid_identifier?("Not/AZone")
  end

  test "runtime config requirements return clean errors for invalid scopes" do
    assert_raise ArgumentError, "runtime config scope must be an atom, got: \"source\"", fn ->
      Requirements.normalize!(%{"source" => %{token: Ref.secret_env!("SOURCE_TOKEN")}})
    end
  end

  test "runtime config requirements merge fields and deduplicate identical refs" do
    token = Ref.secret_env!("SOURCE_TOKEN")

    assert Requirements.merge!(
             %{source: %{url: Ref.env!("SOURCE_URL"), token: token}},
             %{source: %{username: Ref.env!("SOURCE_USERNAME"), token: token}}
           ) == %{
             source: %{
               url: Ref.env!("SOURCE_URL"),
               username: Ref.env!("SOURCE_USERNAME"),
               token: token
             }
           }
  end

  test "runtime config requirements reject conflicting duplicate fields in one declaration" do
    assert_raise ArgumentError, ~r/conflicting runtime config :source.token/, fn ->
      Requirements.normalize!(
        source: [
          token: Ref.secret_env!("SOURCE_TOKEN"),
          token: Ref.secret_env!("OTHER_SOURCE_TOKEN")
        ]
      )
    end
  end

  test "runtime config requirements deduplicate identical fields in one declaration" do
    token = Ref.secret_env!("SOURCE_TOKEN")

    assert Requirements.normalize!(source: [token: token, token: token]) ==
             %{source: %{token: token}}
  end

  test "runtime config requirement conflicts include bundle origins without values" do
    left =
      Bundle.new!(:github, [api_key: Ref.secret_env!("GITHUB_API_KEY")],
        module: MyApp.PlatformConfig,
        file: "platform_config.ex",
        line: 10
      )

    right =
      Bundle.new!(:github, [api_key: Ref.secret_env!("OTHER_GITHUB_API_KEY")],
        module: MyApp.GitHubConfig,
        file: "github_config.ex",
        line: 20
      )

    assert_raise ArgumentError,
                 ~r/conflicting runtime config :github.api_key.*PlatformConfig.*GitHubConfig/s,
                 fn ->
                   Requirements.merge_all!([left, right])
                 end
  end

  test "secret env references support optional values without losing secrecy" do
    assert Ref.secret_env!("OPTIONAL_TOKEN", required?: false) ==
             %Ref{provider: :env, key: "OPTIONAL_TOKEN", secret?: true, required?: false}
  end

  test "invalid runtime config diagnostics do not inspect supplied values" do
    resolved_secret = "resolved-super-secret"

    bundle_error =
      assert_raise ArgumentError, fn ->
        Bundle.validate!(%{token: resolved_secret})
      end

    requirements_error =
      assert_raise ArgumentError, fn ->
        Requirements.normalize!(source: [token: resolved_secret])
      end

    refute bundle_error.message =~ resolved_secret
    refute requirements_error.message =~ resolved_secret
  end
end
