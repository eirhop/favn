defmodule FavnView.RebuildsLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
  alias FavnView.Components.RebuildPage
  alias FavnView.CommandAttempt

  @page_size 100

  @impl true
  def mount(params, _session, socket) do
    {operations, page, error} = load_operations(context(socket), [])

    {:ok,
     assign(socket,
       operations: operations,
       next_cursor: page && page.next_cursor,
       has_more?: page && page.has_more?,
       target_id: Map.get(params, "target_id", ""),
       plan: nil,
       plan_attempt: nil,
       start_attempt: nil,
       planning?: false,
       error: error
     )}
  end

  @impl true
  def handle_event(
        "plan_rebuild",
        %{"rebuild" => %{"target_id" => target_id, "reason" => reason} = rebuild} = params,
        socket
      ) do
    socket = assign(socket, planning?: true, error: nil, plan: nil, target_id: target_id)

    attempt =
      CommandAttempt.next(
        socket.assigns.plan_attempt,
        "rebuild_plan",
        {target_id, reason, rebuild},
        params
      )

    socket = assign(socket, :plan_attempt, attempt)

    options = [
      combine_windows: Map.get(rebuild, "combine_windows", "true") == "true",
      empty: Map.get(rebuild, "empty", "false") == "true"
    ]

    case plan_rebuild(context(socket), target_id, reason, attempt.key, options) do
      {:ok, plan} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> assign(planning?: false, plan: plan, plan_attempt: nil)}

      {:error, failure} ->
        {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, failure)

        {:noreply,
         assign(socket, planning?: false, error: error_label(failure), plan_attempt: attempt)}
    end
  end

  def handle_event("start_rebuild", params, %{assigns: %{plan: plan}} = socket)
      when is_map(plan) do
    attempt =
      CommandAttempt.next(socket.assigns.start_attempt, "rebuild_start", plan.plan_id, params)

    socket = assign(socket, :start_attempt, attempt)

    case start_rebuild(context(socket), plan.plan_id, plan.plan_hash, attempt.key) do
      {:ok, operation} ->
        {:noreply,
         socket
         |> CommandAttempt.acknowledge(attempt)
         |> push_navigate(to: ~p"/rebuilds/#{operation.operation_id}")}

      {:error, failure} ->
        {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, failure)
        {:noreply, assign(socket, error: error_label(failure), start_attempt: attempt)}
    end
  end

  def handle_event("start_rebuild", _params, socket), do: {:noreply, socket}

  def handle_event("load_more", _params, %{assigns: %{next_cursor: cursor}} = socket)
      when not is_nil(cursor) do
    {operations, page, error} = load_operations(context(socket), after: cursor)

    {:noreply,
     assign(socket,
       operations: socket.assigns.operations ++ operations,
       next_cursor: page && page.next_cursor,
       has_more?: page && page.has_more?,
       error: error
     )}
  end

  def handle_event("load_more", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <RebuildPage.rebuilds_page
      operations={@operations}
      plan={@plan}
      target_id={@target_id}
      error={@error}
      has_more?={@has_more? || false}
      planning?={@planning?}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    />
    """
  end

  defp load_operations(operator_context, opts) do
    case page_rebuilds(operator_context, Keyword.put(opts, :limit, @page_size)) do
      {:ok, page} ->
        {page.items, page, nil}

      {:error, _failure} ->
        Logger.error("rebuilds.list failed")
        {[], nil, "Rebuild operations are temporarily unavailable."}
    end
  end

  defp context(socket), do: socket.assigns.current_scope.operator_context

  defp page_rebuilds(context, opts) do
    Application.get_env(
      :favn_view,
      :page_operator_rebuilds_fun,
      &Orchestrator.page_operator_rebuilds/2
    ).(context, opts)
  end

  defp plan_rebuild(context, target_id, reason, idempotency_key, options) do
    Application.get_env(
      :favn_view,
      :plan_operator_rebuild_fun,
      &Orchestrator.plan_operator_rebuild/4
    ).(
      context,
      target_id,
      reason,
      Keyword.put(options, :idempotency_key, idempotency_key)
    )
  end

  defp start_rebuild(context, plan_id, plan_hash, idempotency_key) do
    Application.get_env(
      :favn_view,
      :start_operator_rebuild_fun,
      &Orchestrator.start_operator_rebuild/4
    ).(context, plan_id, plan_hash, idempotency_key: idempotency_key)
  end

  defp error_label(:forbidden), do: "Administrator access is required."
  defp error_label(_failure), do: "The rebuild request could not be completed."
end
