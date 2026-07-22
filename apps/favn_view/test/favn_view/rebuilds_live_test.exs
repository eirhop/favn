defmodule FavnView.RebuildsLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias FavnOrchestrator.Auth.Session
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Results.Session, as: SessionResult
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnView.RebuildsLive
  alias FavnView.RebuildDetailLive

  defmodule IdentityStore do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Queries.GetActor
    alias FavnOrchestrator.Persistence.Queries.GetSession
    alias FavnOrchestrator.Persistence.Selectors.ActorById
    alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash

    def get_session(%GetSession{
          workspace_context: context,
          selector: %SessionByTokenHash{token_hash: token_hash}
        }) do
      fetch(:view_rebuild_sessions, {context.workspace_id, token_hash})
    end

    def get_actor(%GetActor{
          workspace_context: context,
          selector: %ActorById{actor_id: actor_id}
        }) do
      fetch(:view_rebuild_actors, {context.workspace_id, actor_id})
    end

    defp fetch(key, identity) do
      case :ets.lookup(:view_rebuild_identities, {key, identity}) do
        [{{^key, ^identity}, value}] -> {:ok, value}
        [] -> {:error, Error.new(:not_found, "identity not found")}
      end
    end
  end

  @env_keys [
    :page_operator_rebuilds_fun,
    :plan_operator_rebuild_fun,
    :start_operator_rebuild_fun,
    :get_operator_rebuild_fun,
    :page_operator_rebuild_items_fun,
    :cancel_operator_rebuild_fun,
    :retry_operator_rebuild_fun,
    :reconcile_operator_rebuild_fun
  ]

  setup do
    previous = Map.new(@env_keys, &{&1, Application.get_env(:favn_view, &1)})

    Application.put_env(:favn_view, :page_operator_rebuilds_fun, fn :operator_context, opts ->
      send(self(), {:page_rebuilds, opts})
      {:ok, %{items: [], next_cursor: nil, has_more?: false, limit: 100}}
    end)

    stores =
      Stores.__struct__()
      |> Map.from_struct()
      |> Map.new(fn {name, _module} -> {name, IdentityStore} end)
      |> then(&struct!(Stores, &1))

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{backend: __MODULE__, options: [], stores: stores})

    :ets.new(:view_rebuild_identities, [:named_table, :public, :set])

    on_exit(fn ->
      if Process.alive?(runtime), do: GenServer.stop(runtime)

      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_view, key)
        {key, value} -> Application.put_env(:favn_view, key, value)
      end)
    end)

    :ok
  end

  @tag :browser
  test "mounted operator route completes plan review and start", %{conn: conn} do
    test_pid = self()
    {conn, operator_context} = authenticated_conn(conn)
    plan_hash = String.duplicate("a", 64)

    Application.put_env(:favn_view, :page_operator_rebuilds_fun, fn ^operator_context, opts ->
      send(test_pid, {:mounted_page_rebuilds, opts})
      {:ok, %{items: [], next_cursor: nil, has_more?: false, limit: 100}}
    end)

    Application.put_env(:favn_view, :plan_operator_rebuild_fun, fn
      ^operator_context, "asset:orders", "schema changed" ->
        send(test_pid, :mounted_plan)

        {:ok,
         %{
           plan_id: "rebuild-browser-plan",
           plan_hash: plan_hash,
           expires_at: DateTime.add(DateTime.utc_now(), 3_600, :second),
           payload: %{root_target_id: "asset:orders", item_count: 1},
           permissions: %{start: true}
         }}
    end)

    Application.put_env(:favn_view, :start_operator_rebuild_fun, fn
      ^operator_context, "rebuild-browser-plan", ^plan_hash ->
        send(test_pid, :mounted_start)
        {:ok, %{operation_id: "rebuild-browser-plan"}}
    end)

    assert {:ok, view, _html} = live(conn, ~p"/rebuilds")
    assert_receive {:mounted_page_rebuilds, [limit: 100]}
    assert has_element?(view, "[data-testid=rebuilds-page]")

    view
    |> form("form[phx-submit=plan_rebuild]",
      rebuild: %{target_id: "asset:orders", reason: "schema changed"}
    )
    |> render_submit()

    assert_receive :mounted_plan
    assert has_element?(view, "[data-testid=rebuild-plan]")
    assert has_element?(view, "[data-testid=start-rebuild]")

    view
    |> element("[data-testid=start-rebuild]")
    |> render_click()

    assert_receive :mounted_start
    assert_redirect(view, "/rebuilds/rebuild-browser-plan")
  end

  test "manual workflow plans first and starts only the reviewed plan" do
    test_pid = self()

    Application.put_env(:favn_view, :plan_operator_rebuild_fun, fn
      :operator_context, "asset:orders", "schema changed" ->
        send(test_pid, :planned_through_facade)

        {:ok,
         %{
           plan_id: "rebuild-plan-1",
           plan_hash: String.duplicate("a", 64),
           expires_at: ~U[2026-07-22 14:00:00Z]
         }}
    end)

    Application.put_env(:favn_view, :start_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1", plan_hash ->
        send(test_pid, {:started_through_facade, plan_hash})
        {:ok, %{operation_id: "rebuild-plan-1"}}
    end)

    socket = socket()
    assert {:ok, mounted} = RebuildsLive.mount(%{"target_id" => "asset:orders"}, %{}, socket)
    assert mounted.assigns.plan == nil

    assert {:noreply, planned} =
             RebuildsLive.handle_event(
               "plan_rebuild",
               %{"rebuild" => %{"target_id" => "asset:orders", "reason" => "schema changed"}},
               mounted
             )

    assert_received :planned_through_facade
    assert planned.assigns.plan.plan_id == "rebuild-plan-1"

    assert {:noreply, _started} = RebuildsLive.handle_event("start_rebuild", %{}, planned)
    assert_received {:started_through_facade, plan_hash}
    assert plan_hash == String.duplicate("a", 64)
  end

  test "detail workflow delegates mutation permissions and item pagination to the facade" do
    test_pid = self()
    plan_hash = String.duplicate("a", 64)

    operation = %{
      operation_id: "rebuild-plan-1",
      root_target_id: "asset:orders",
      plan_hash: plan_hash,
      state: :activation_unknown,
      permissions: %{start: true, cancel: true, retry: true, reconcile: true}
    }

    Application.put_env(:favn_view, :get_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1" -> {:ok, operation}
    end)

    Application.put_env(:favn_view, :page_operator_rebuild_items_fun, fn
      :operator_context, "rebuild-plan-1", opts ->
        send(test_pid, {:page_items, opts})

        if Keyword.has_key?(opts, :after) do
          {:ok, %{items: [%{item_id: "item-2"}], next_cursor: nil, has_more?: false}}
        else
          {:ok,
           %{
             items: [%{item_id: "item-1"}],
             next_cursor: %{ordinal: 0, target_id: "asset:orders", item_id: "item-1"},
             has_more?: true
           }}
        end
    end)

    Application.put_env(:favn_view, :start_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1", ^plan_hash ->
        send(test_pid, :started_rebuild)
        {:ok, operation}
    end)

    Application.put_env(:favn_view, :cancel_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1", "operator request" ->
        send(test_pid, :cancelled_rebuild)
        {:ok, operation}
    end)

    Application.put_env(:favn_view, :retry_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1", ^plan_hash ->
        send(test_pid, :retried_rebuild)
        {:ok, operation}
    end)

    Application.put_env(:favn_view, :reconcile_operator_rebuild_fun, fn
      :operator_context, "rebuild-plan-1" ->
        send(test_pid, :reconciled_rebuild)
        {:ok, operation}
    end)

    assert {:ok, mounted} =
             RebuildDetailLive.mount(%{"operation_id" => "rebuild-plan-1"}, %{}, socket())

    assert_receive {:page_items, [limit: 100]}
    assert mounted.assigns.items == [%{item_id: "item-1"}]

    assert {:noreply, paged} = RebuildDetailLive.handle_event("load_more_items", %{}, mounted)
    assert_receive {:page_items, page_opts}
    assert Keyword.has_key?(page_opts, :after)
    assert Enum.map(paged.assigns.items, & &1.item_id) == ["item-1", "item-2"]

    assert {:noreply, _socket} =
             RebuildDetailLive.handle_event(
               "cancel_rebuild",
               %{"cancel" => %{"reason" => "operator request"}},
               mounted
             )

    assert_receive :cancelled_rebuild

    assert {:noreply, _socket} =
             RebuildDetailLive.handle_event("retry_rebuild", %{}, mounted)

    assert_receive :retried_rebuild

    assert {:noreply, _socket} =
             RebuildDetailLive.handle_event("reconcile_rebuild", %{}, mounted)

    assert_receive :reconciled_rebuild

    assert {:noreply, _socket} =
             RebuildDetailLive.handle_event("start_rebuild", %{}, mounted)

    assert_receive :started_rebuild
  end

  defp socket do
    %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        current_scope: %{operator_context: :operator_context}
      }
    }
  end

  defp authenticated_conn(conn) do
    workspace_id = "workspace-browser"
    actor_id = "actor-browser-admin"
    token = Session.raw_token()
    now = DateTime.utc_now()

    actor = %Actor{
      actor_id: actor_id,
      username: "browser-admin",
      display_name: "Browser Admin",
      status: :active,
      workspace_id: workspace_id,
      membership_status: :active,
      roles: [:workspace_admin],
      access_version: 1,
      version: 1
    }

    session = %SessionResult{
      session_id: "session-browser-admin",
      actor_id: actor_id,
      provider: "password_local",
      issued_at: now,
      status: :active,
      expires_at: DateTime.add(now, 3_600, :second)
    }

    :ets.insert(
      :view_rebuild_identities,
      {{:view_rebuild_actors, {workspace_id, actor_id}}, actor}
    )

    :ets.insert(
      :view_rebuild_identities,
      {{:view_rebuild_sessions, {workspace_id, Session.token_hash(token)}}, session}
    )

    {:ok, operator_context} =
      FavnOrchestrator.operator_context(workspace_id, actor_map(actor), session_map(session))

    conn =
      init_test_session(conn, %{
        operator_workspace_id: workspace_id,
        operator_session_token: token
      })

    {conn, operator_context}
  end

  defp actor_map(actor) do
    %{
      id: actor.actor_id,
      username: actor.username,
      display_name: actor.display_name,
      roles: [:admin],
      status: :active,
      workspace_id: actor.workspace_id,
      access_version: actor.access_version
    }
  end

  defp session_map(session) do
    %{
      id: session.session_id,
      actor_id: session.actor_id,
      provider: session.provider,
      issued_at: session.issued_at,
      expires_at: session.expires_at,
      revoked_at: session.revoked_at
    }
  end
end
