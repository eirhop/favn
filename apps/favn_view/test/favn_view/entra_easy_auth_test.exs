defmodule FavnView.EntraEasyAuthTest do
  use FavnView.ConnCase, async: false

  alias FavnView.Auth
  alias FavnView.Auth.AzureContainerAppsEntra
  alias FavnView.ProductionRuntimeConfig

  @tenant_id "11111111-1111-4111-8111-111111111111"
  @object_id "22222222-2222-4222-8222-222222222222"

  setup do
    persistent_key = {ProductionRuntimeConfig, :config}
    previous_runtime = :persistent_term.get(persistent_key, :missing)
    previous_auth = Application.get_env(:favn_view, :operator_auth)
    previous_login = Application.get_env(:favn_view, :operator_external_login_fun)
    :persistent_term.erase(persistent_key)

    on_exit(fn ->
      restore_env(:operator_auth, previous_auth)
      restore_env(:operator_external_login_fun, previous_login)

      case previous_runtime do
        :missing -> :persistent_term.erase(persistent_key)
        config -> :persistent_term.put(persistent_key, config)
      end
    end)

    :ok
  end

  test "accepts only the immutable tenant and object claims" do
    encoded =
      principal([
        %{"typ" => "tid", "val" => String.upcase(@tenant_id)},
        %{"typ" => "oid", "val" => String.upcase(@object_id)},
        %{"typ" => "preferred_username", "val" => "mutable@example.com"},
        %{"typ" => "roles", "val" => "Global Administrator"}
      ])

    assert {:ok,
            %{
              provider: "azure_container_apps_entra",
              tenant_id: @tenant_id,
              subject_id: @object_id
            }} = AzureContainerAppsEntra.from_header_value(encoded, @tenant_id)
  end

  test "fails closed for malformed, wrong-tenant, missing, or conflicting principals" do
    valid_claims = [
      %{"typ" => "tid", "val" => @tenant_id},
      %{"typ" => "oid", "val" => @object_id}
    ]

    assert {:error, :invalid_principal} =
             AzureContainerAppsEntra.from_header_value("not-base64", @tenant_id)

    assert {:error, :invalid_principal} =
             AzureContainerAppsEntra.from_header_value(
               principal([%{"typ" => "oid", "val" => @object_id}]),
               @tenant_id
             )

    assert {:error, :invalid_principal} =
             AzureContainerAppsEntra.from_header_value(
               principal(valid_claims),
               "33333333-3333-4333-8333-333333333333"
             )

    assert {:error, :invalid_principal} =
             AzureContainerAppsEntra.from_header_value(
               principal([
                 %{"typ" => "tid", "val" => @tenant_id},
                 %{"typ" => "oid", "val" => @object_id},
                 %{"typ" => "oid", "val" => "33333333-3333-4333-8333-333333333333"}
               ]),
               @tenant_id
             )

    assert {:error, :invalid_principal} =
             AzureContainerAppsEntra.from_header_value(
               String.duplicate("a", 16_385),
               @tenant_id
             )
  end

  test "login exchanges the Easy Auth principal for a Favn session" do
    parent = self()

    Application.put_env(:favn_view, :operator_auth, %{
      mode: :azure_container_apps_entra,
      tenant_id: @tenant_id,
      workspace_id: "workspace-1"
    })

    Application.put_env(:favn_view, :operator_external_login_fun, fn workspace_id, identity ->
      send(parent, {:external_login, workspace_id, identity})

      {:ok,
       %{
         id: "ses-1",
         token: String.duplicate("a", 43)
       }, %{id: "actor-1"}}
    end)

    conn =
      build_conn()
      |> put_req_header("x-ms-client-principal", principal_header())
      |> get("/login")

    assert redirected_to(conn) == "/assets"

    assert_received {:external_login, "workspace-1",
                     %{
                       provider: "azure_container_apps_entra",
                       tenant_id: @tenant_id,
                       subject_id: @object_id
                     }}
  end

  test "missing or unlinked principals get only a generic denial" do
    Application.put_env(:favn_view, :operator_auth, %{
      mode: :azure_container_apps_entra,
      tenant_id: @tenant_id,
      workspace_id: "workspace-1"
    })

    response = get(build_conn(), "/login")
    assert response.status == 403
    assert response.resp_body == "Access denied"

    Application.put_env(:favn_view, :operator_external_login_fun, fn _workspace, _identity ->
      {:error, :invalid_credentials}
    end)

    response =
      build_conn()
      |> put_req_header("x-ms-client-principal", principal_header())
      |> get("/login")

    assert response.status == 403
    assert response.resp_body == "Access denied"
    refute response.resp_body =~ @object_id
  end

  test "Orchestrator transport loss reports temporary sign-in unavailability" do
    Application.put_env(:favn_view, :operator_auth, %{
      mode: :azure_container_apps_entra,
      tenant_id: @tenant_id,
      workspace_id: "workspace-1"
    })

    Application.put_env(:favn_view, :operator_external_login_fun, fn _workspace, _identity ->
      {:error, :orchestrator_outcome_unknown}
    end)

    response =
      build_conn()
      |> put_req_header("x-ms-client-principal", principal_header())
      |> get("/login")

    assert response.status == 503
    assert response.resp_body == "Sign-in service unavailable"
  end

  test "Entra mode disables password login and signs out through Easy Auth" do
    Application.put_env(:favn_view, :operator_auth, %{
      mode: :azure_container_apps_entra,
      tenant_id: @tenant_id,
      workspace_id: "workspace-1"
    })

    response =
      build_conn()
      |> post("/login", %{
        "operator" => %{
          "workspace_id" => "workspace-1",
          "username" => "operator",
          "password" => "must-not-be-accepted"
        }
      })

    assert response.status == 403
    assert response.resp_body == "Access denied"

    conn =
      build_conn()
      |> init_test_session(%{})
      |> Phoenix.Controller.fetch_flash([])
      |> Auth.log_out_operator()

    assert redirected_to(conn) == "/.auth/logout"
  end

  defp principal_header do
    principal([
      %{
        "typ" => "http://schemas.microsoft.com/identity/claims/tenantid",
        "val" => @tenant_id
      },
      %{
        "typ" => "http://schemas.microsoft.com/identity/claims/objectidentifier",
        "val" => @object_id
      }
    ])
  end

  defp principal(claims) do
    %{"auth_typ" => "aad", "claims" => claims}
    |> Jason.encode!()
    |> Base.encode64()
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
