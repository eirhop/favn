defmodule FavnStoragePostgres.StorageV2.WorkspaceProvisioningTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Ecto.Adapters.SQL.Sandbox
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.WorkspaceProvisioning
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AuthActor
  alias FavnStoragePostgres.Schemas.AuthCredential
  alias FavnStoragePostgres.Schemas.AuthExternalIdentity
  alias FavnStoragePostgres.Schemas.AuthWorkspaceMembership
  alias FavnStoragePostgres.Schemas.Workspace
  alias FavnStoragePostgres.Schemas.WorkspaceProvisioningOperation
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.WorkspaceProvisioning.Store

  @fingerprint_key "workspace-provisioning-test-key-0001"

  defmodule InjectedStore do
    @behaviour FavnOrchestrator.Persistence.WorkspaceProvisioningStore

    alias FavnStoragePostgres.WorkspaceProvisioning.Store

    @impl true
    def provision(command) do
      Store.provision(command, after_step: Process.get(:workspace_provisioning_after_step))
    end

    @impl true
    def get(query), do: Store.get(query)
  end

  defmodule LostResponseStore do
    @behaviour FavnOrchestrator.Persistence.WorkspaceProvisioningStore

    alias FavnOrchestrator.Persistence.Error
    alias FavnStoragePostgres.WorkspaceProvisioning.Store

    @impl true
    def provision(command) do
      {:ok, _committed} = Store.provision(command)
      {:error, Error.new(:unavailable, "injected lost response", retryable?: true)}
    end

    @impl true
    def get(query), do: Store.get(query)
  end

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} =
      Config.repo_options(url: url, ssl_mode: :disable, pool: Sandbox, pool_size: 2)

    start_supervised!({Repo, options})
    start_supervised!({Lifecycle, shutdown_drain_timeout_ms: 120_000})
    start_supervised!(AuthStore)
    :ok = Lifecycle.mark_accepting()
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
  end

  setup do
    :ok = Sandbox.checkout(Repo)
    Process.delete(:workspace_provisioning_after_step)
    :ok
  end

  test "schema diagnostics include the provisioning receipt contract" do
    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    assert diagnostics.definition_fingerprint_matches?
    assert diagnostics.missing_tables == []
    assert diagnostics.missing_columns == []
  end

  test "Entra provisioning is atomic, exactly replayable, and reconcilable" do
    input = entra_input(unique("entra"))

    assert {:ok, first} = provision(input)
    refute first.replayed?
    assert first.status == :ready
    assert first.authentication_mode == :entra
    assert first.workspace_roles == [:workspace_admin]
    assert first.platform_roles == [:platform_admin]

    assert {:ok, replay} = provision(input)
    assert replay.replayed?
    assert Map.delete(first, :replayed?) == Map.delete(replay, :replayed?)

    assert {:ok, reconciled} =
             WorkspaceProvisioning.status(first.workspace_id, store: Store)

    assert reconciled.status == :ready
    assert reconciled.actor_id == first.actor_id
    refute inspect(reconciled) =~ input["administrator"]["object_id"]
    refute inspect(reconciled) =~ input["administrator"]["tenant_id"]

    assert %AuthExternalIdentity{provider: "azure_container_apps_entra"} =
             Repo.get_by(AuthExternalIdentity, actor_id: first.actor_id)

    assert {:ok, session, actor} =
             FavnOrchestrator.operator_external_login(first.workspace_id, %{
               provider: "azure_container_apps_entra",
               tenant_id: input["administrator"]["tenant_id"],
               subject_id: input["administrator"]["object_id"]
             })

    assert actor.id == first.actor_id
    assert is_binary(session.token)

    Repo.insert!(%AuthExternalIdentity{
      provider: "azure_container_apps_entra",
      tenant_id: "33333333-3333-3333-3333-333333333333",
      subject_id: "44444444-4444-4444-4444-444444444444",
      actor_id: first.actor_id,
      linked_at: DateTime.utc_now(),
      inserted_at: DateTime.utc_now()
    })

    assert {:ok, %{status: :ready}} =
             WorkspaceProvisioning.status(first.workspace_id, store: Store)

    Repo.delete_all(
      from(identity in AuthExternalIdentity,
        where:
          identity.actor_id == ^first.actor_id and
            identity.tenant_id == ^input["administrator"]["tenant_id"]
      )
    )

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :constraint}} =
             WorkspaceProvisioning.status(first.workspace_id, store: Store)
  end

  test "password provisioning persists only the verifier and returns no secret" do
    password = "correct horse battery staple 634"
    input = password_input(unique("password"), password)

    assert {:ok, result} = provision(input)
    assert result.authentication_mode == :password
    refute inspect(result) =~ password

    assert %AuthCredential{password_hash: hash, algorithm: "argon2id"} =
             Repo.get(AuthCredential, result.actor_id)

    refute hash == password
    assert Argon2.verify_pass(password, hash)
    refute Repo.get_by(AuthExternalIdentity, actor_id: result.actor_id)

    assert {:ok, session, actor} =
             FavnOrchestrator.operator_password_login(
               result.workspace_id,
               result.username,
               password,
               %{}
             )

    assert actor.id == result.actor_id
    assert is_binary(session.token)
  end

  test "administrator authentication is an exact tagged choice" do
    input = entra_input(unique("tagged-choice"))

    assert {:error, :multiple_administrator_modes} =
             input
             |> put_in(["administrator", "password"], "must-not-be-used")
             |> provision()

    assert {:error, :exactly_one_administrator_mode_required} =
             input
             |> update_in(["administrator"], &Map.delete(&1, "mode"))
             |> provision()

    assert Repo.get(Workspace, input["workspace"]["id"]) == nil
  end

  test "reusing an operation identity with changed details is a bounded conflict" do
    input = entra_input(unique("conflict"))
    assert {:ok, ready} = provision(input)

    changed = put_in(input, ["administrator", "display_name"], "Different administrator")

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :conflict} = error} =
             provision(changed)

    assert error.details.reason_code == "workspace_provisioning_conflict"
    actor_id = ready.actor_id

    assert Repo.one(
             from(actor in AuthActor,
               where: actor.actor_id == ^actor_id,
               select: count(actor.actor_id)
             )
           ) == 1

    assert Repo.get(WorkspaceProvisioningOperation, input["operation_id"]).actor_id ==
             ready.actor_id
  end

  test "crossed operation and workspace identities return a bounded conflict" do
    first_input = entra_input(unique("crossed-first"))
    second_input = entra_input(unique("crossed-second"))
    assert {:ok, _first} = provision(first_input)
    assert {:ok, _second} = provision(second_input)

    crossed =
      first_input
      |> Map.put("workspace", second_input["workspace"])
      |> Map.put("administrator", second_input["administrator"])

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :conflict} = error} =
             provision(crossed)

    assert error.details.reason_code == "workspace_provisioning_conflict"
  end

  test "status reconciles a committed operation after the client loses its response" do
    input = entra_input(unique("lost-response"))
    workspace_id = input["workspace"]["id"]

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :unavailable}} =
             WorkspaceProvisioning.provision(input,
               store: LostResponseStore,
               fingerprint_key: @fingerprint_key
             )

    assert {:ok, %{workspace_id: ^workspace_id, status: :ready}} =
             WorkspaceProvisioning.status(workspace_id, store: LostResponseStore)
  end

  test "an exact existing Entra actor can administer another new workspace" do
    first_input = entra_input(unique("shared-entra"))
    assert {:ok, first} = provision(first_input)

    second_identity = unique("shared-entra-workspace")

    second_input =
      first_input
      |> Map.put("operation_id", "operation-#{second_identity}")
      |> Map.put("workspace", %{
        "id" => "workspace-#{second_identity}",
        "slug" => "workspace-#{second_identity}",
        "display_name" => "Workspace #{second_identity}"
      })

    assert {:ok, second} = provision(second_input)
    assert second.actor_id == first.actor_id
    assert second.workspace_id != first.workspace_id

    assert Repo.one(
             from(actor in AuthActor,
               where: actor.actor_id == ^first.actor_id,
               select: count(actor.actor_id)
             )
           ) == 1
  end

  test "an existing actor cannot change or add its selected authentication mode" do
    password = "cross-mode-password-long-enough"
    password_input = password_input(unique("cross-mode"), password)
    assert {:ok, password_actor} = provision(password_input)

    entra_workspace = entra_input(unique("cross-mode-entra"))["workspace"]

    entra_for_password_actor = %{
      "operation_id" => "cross-mode-entra-operation",
      "workspace" => entra_workspace,
      "administrator" => %{
        "mode" => "entra",
        "username" => password_actor.username,
        "display_name" => password_input["administrator"]["display_name"],
        "tenant_id" => "11111111-1111-1111-1111-111111111111",
        "object_id" => "55555555-5555-4555-8555-555555555555"
      }
    }

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :conflict}} =
             provision(entra_for_password_actor)

    assert is_nil(Repo.get(Workspace, entra_workspace["id"]))

    entra_input = entra_input(unique("changed-entra"))
    assert {:ok, entra_actor} = provision(entra_input)

    changed_workspace = entra_input(unique("changed-entra-workspace"))["workspace"]

    changed_identity =
      entra_input
      |> Map.put("operation_id", "changed-entra-operation")
      |> Map.put("workspace", changed_workspace)
      |> put_in(["administrator", "object_id"], "66666666-6666-4666-8666-666666666666")

    assert {:error, %FavnOrchestrator.Persistence.Error{kind: :conflict}} =
             provision(changed_identity)

    assert is_nil(Repo.get(Workspace, changed_workspace["id"]))
    assert Repo.get(AuthActor, entra_actor.actor_id)
  end

  test "a coherent legacy workspace remains ready without a new receipt" do
    input = entra_input(unique("legacy-ready"))
    assert {:ok, ready} = provision(input)
    Repo.delete!(Repo.get!(WorkspaceProvisioningOperation, input["operation_id"]))

    assert {:ok, legacy} = WorkspaceProvisioning.status(ready.workspace_id, store: Store)
    assert legacy.status == :ready
    assert legacy.operation_id == "legacy:" <> ready.workspace_id
    assert legacy.actor_id == ready.actor_id
  end

  test "every persistence boundary rolls back the complete authorization state" do
    for stage <- [:workspace, :actor, :membership, :platform_grant, :authentication, :receipt] do
      identity = unique(Atom.to_string(stage))
      input = entra_input(identity)
      username = input["administrator"]["username"]

      Process.put(:workspace_provisioning_after_step, fn
        ^stage -> {:error, :injected_failure}
        _completed -> :ok
      end)

      assert {:error, _failure} =
               WorkspaceProvisioning.provision(input,
                 store: InjectedStore,
                 fingerprint_key: @fingerprint_key
               )

      assert is_nil(Repo.get(Workspace, input["workspace"]["id"]))
      assert is_nil(Repo.get(WorkspaceProvisioningOperation, input["operation_id"]))

      assert Repo.one(
               from(actor in AuthActor,
                 where: actor.normalized_username == ^String.downcase(username),
                 select: count(actor.actor_id)
               )
             ) == 0

      assert Repo.one(
               from(membership in AuthWorkspaceMembership,
                 where: membership.workspace_id == ^input["workspace"]["id"],
                 select: count(membership.actor_id)
               )
             ) == 0
    end
  end

  defp provision(input) do
    WorkspaceProvisioning.provision(input, store: Store, fingerprint_key: @fingerprint_key)
  end

  defp entra_input(identity) do
    %{
      "operation_id" => "operation-#{identity}",
      "workspace" => %{
        "id" => "workspace-#{identity}",
        "slug" => "workspace-#{identity}",
        "display_name" => "Workspace #{identity}"
      },
      "administrator" => %{
        "mode" => "entra",
        "username" => "admin-#{identity}",
        "display_name" => "Administrator #{identity}",
        "tenant_id" => "11111111-1111-1111-1111-111111111111",
        "object_id" => uuid(identity)
      }
    }
  end

  defp password_input(identity, password) do
    input = entra_input(identity)

    Map.put(input, "administrator", %{
      "mode" => "password",
      "username" => "admin-#{identity}",
      "display_name" => "Administrator #{identity}",
      "password" => password
    })
  end

  defp unique(prefix),
    do: "#{prefix}-#{System.unique_integer([:positive])}"

  defp uuid(identity) do
    suffix =
      identity |> :erlang.phash2(0xFFFFFF) |> Integer.to_string(16) |> String.pad_leading(6, "0")

    String.downcase("22222222-2222-4222-8222-222222#{suffix}")
  end
end
