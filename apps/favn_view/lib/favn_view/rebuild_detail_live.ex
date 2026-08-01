defmodule FavnView.RebuildDetailLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Components.RebuildPage
  alias FavnView.Components.AppShell
  alias FavnView.CommandAttempt
  alias FavnView.LiveRefresh

  @page_size 100
  @refresh_interval_ms 1_500
  @terminal_states [:succeeded, :failed, :cancelled]

  @impl true
  def mount(%{"operation_id" => operation_id}, _session, socket) do
    {operation, items, page, error} = load(operation_id, context(socket))

    socket =
      socket
      |> assign(
        operation_id: operation_id,
        operation: operation,
        items: items,
        items_next_cursor: page && page.next_cursor,
        items_has_more?: page && page.has_more?,
        command_attempts: %{},
        error: error
      )
      |> LiveRefresh.init([:rebuild_poll_ref])
      |> maybe_schedule_poll()

    {:ok, socket}
  end

  @impl true
  def handle_info({:poll_rebuild, token}, socket) do
    case LiveRefresh.take(socket, :rebuild_poll_ref, token) do
      {:ok, socket} -> {:noreply, socket |> refresh() |> maybe_schedule_poll()}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  @impl true
  def handle_event("start_rebuild", params, socket) do
    mutate(socket, "rebuild_start", socket.assigns.operation_id, params, fn operation, key ->
      start_rebuild(
        context(socket),
        operation.operation_id,
        operation.plan_hash,
        key
      )
    end)
  end

  def handle_event(
        "cancel_rebuild",
        %{"cancel" => %{"reason" => reason}} = params,
        socket
      ) do
    mutate(
      socket,
      "rebuild_cancel",
      {socket.assigns.operation_id, reason},
      params,
      fn operation, key ->
        cancel_rebuild(
          context(socket),
          operation.operation_id,
          reason,
          key
        )
      end
    )
  end

  def handle_event("retry_rebuild", params, socket) do
    mutate(socket, "rebuild_retry", socket.assigns.operation_id, params, fn operation, key ->
      retry_rebuild(
        context(socket),
        operation.operation_id,
        operation.plan_hash,
        key
      )
    end)
  end

  def handle_event("reconcile_rebuild", params, socket) do
    mutate(socket, "rebuild_reconcile", socket.assigns.operation_id, params, fn operation, key ->
      reconcile_rebuild(context(socket), operation.operation_id, key)
    end)
  end

  def handle_event("load_more_items", _params, %{assigns: %{items_next_cursor: cursor}} = socket)
      when not is_nil(cursor) do
    case page_items(
           context(socket),
           socket.assigns.operation_id,
           limit: @page_size,
           after: cursor
         ) do
      {:ok, page} ->
        {:noreply,
         assign(socket,
           items: socket.assigns.items ++ page.items,
           items_next_cursor: page.next_cursor,
           items_has_more?: page.has_more?,
           error: nil
         )}

      {:error, failure} ->
        {:noreply, assign(socket, :error, error_label(failure))}
    end
  end

  def handle_event("load_more_items", _params, socket), do: {:noreply, socket}

  @impl true
  def render(%{operation: operation} = assigns) when is_map(operation) do
    ~H"""
    <RebuildPage.rebuild_detail_page
      operation={@operation}
      items={@items}
      items_has_more?={@items_has_more? || false}
      error={@error}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    />
    """
  end

  def render(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Rebuild unavailable"
      nav_items={RebuildPage.nav_items()}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      back_href={~p"/rebuilds"}
      back_label="Rebuilds"
    >
      <p
        class="mx-auto mt-16 max-w-2xl text-center text-sm favn-text-muted"
        data-testid="rebuild-error"
      >
        {@error}
      </p>
    </AppShell.app_shell>
    """
  end

  defp mutate(socket, command_name, intent, params, command) do
    attempt =
      socket.assigns.command_attempts
      |> Map.get(command_name)
      |> CommandAttempt.next(command_name, intent, params)

    socket =
      assign(
        socket,
        :command_attempts,
        Map.put(socket.assigns.command_attempts, command_name, attempt)
      )

    case command.(socket.assigns.operation, attempt.key) do
      {:ok, updated_operation} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> assign(
           operation: updated_operation,
           error: nil,
           command_attempts: Map.delete(socket.assigns.command_attempts, command_name)
         )
         |> refresh()
         |> maybe_schedule_poll()}

      {:error, failure} ->
        {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, failure)

        {:noreply,
         assign(
           socket,
           error: error_label(failure),
           command_attempts:
             if(attempt,
               do: Map.put(socket.assigns.command_attempts, command_name, attempt),
               else: Map.delete(socket.assigns.command_attempts, command_name)
             )
         )}
    end
  end

  defp refresh(socket) do
    {operation, items, page, error} = load(socket.assigns.operation_id, context(socket))

    assign(socket,
      operation: operation || socket.assigns.operation,
      items: items,
      items_next_cursor: page && page.next_cursor,
      items_has_more?: page && page.has_more?,
      error: error
    )
  end

  defp load(operation_id, operator_context) do
    with {:ok, operation} <- get_rebuild(operator_context, operation_id),
         {:ok, page} <-
           page_items(operator_context, operation_id, limit: @page_size) do
      {operation, page.items, page, nil}
    else
      {:error, failure} ->
        Logger.error("rebuilds.detail failed")
        {nil, [], nil, error_label(failure)}
    end
  end

  defp maybe_schedule_poll(%{assigns: %{operation: %{state: state}}} = socket)
       when state not in @terminal_states do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :rebuild_poll_ref, :poll_rebuild, @refresh_interval_ms)
    else
      socket
    end
  end

  defp maybe_schedule_poll(socket), do: socket
  defp context(socket), do: socket.assigns.current_scope.operator_context

  defp get_rebuild(context, operation_id),
    do:
      configured(:get_operator_rebuild_fun, &FavnOrchestrator.get_operator_rebuild/2).(
        context,
        operation_id
      )

  defp page_items(context, operation_id, opts),
    do:
      configured(
        :page_operator_rebuild_items_fun,
        &FavnOrchestrator.page_operator_rebuild_items/3
      ).(
        context,
        operation_id,
        opts
      )

  defp start_rebuild(context, operation_id, plan_hash, idempotency_key),
    do:
      configured(:start_operator_rebuild_fun, &FavnOrchestrator.start_operator_rebuild/4).(
        context,
        operation_id,
        plan_hash,
        idempotency_key: idempotency_key
      )

  defp cancel_rebuild(context, operation_id, reason, idempotency_key),
    do:
      configured(:cancel_operator_rebuild_fun, &FavnOrchestrator.cancel_operator_rebuild/4).(
        context,
        operation_id,
        reason,
        idempotency_key: idempotency_key
      )

  defp retry_rebuild(context, operation_id, plan_hash, idempotency_key),
    do:
      configured(:retry_operator_rebuild_fun, &FavnOrchestrator.retry_operator_rebuild/4).(
        context,
        operation_id,
        plan_hash,
        idempotency_key: idempotency_key
      )

  defp reconcile_rebuild(context, operation_id, idempotency_key),
    do:
      configured(:reconcile_operator_rebuild_fun, &FavnOrchestrator.reconcile_operator_rebuild/3).(
        context,
        operation_id,
        idempotency_key: idempotency_key
      )

  defp configured(key, default), do: Application.get_env(:favn_view, key, default)

  defp error_label(:forbidden), do: "Administrator access is required."
  defp error_label(_failure), do: "The rebuild request could not be completed."
end
