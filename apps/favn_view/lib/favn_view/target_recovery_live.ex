defmodule FavnView.TargetRecoveryLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.TargetRecoveryPage

  @impl true
  def mount(params, _session, socket) do
    operation = load_operation(context(socket), Map.get(params, "operation_id"))

    {:ok,
     assign(socket,
       target_id: Map.get(params, "target_id", ""),
       plan: nil,
       operation: operation,
       planning?: false,
       error: nil
     )}
  end

  @impl true
  def handle_event(
        "plan_recovery",
        %{"recovery" => %{"target_id" => target_id, "reason" => reason}},
        socket
      ) do
    socket = assign(socket, planning?: true, error: nil, plan: nil, target_id: target_id)

    case plan_recovery(context(socket), target_id, reason) do
      {:ok, plan} ->
        {:noreply, assign(socket, planning?: false, plan: plan)}

      {:error, failure} ->
        {:noreply, assign(socket, planning?: false, error: error_label(failure))}
    end
  end

  def handle_event("start_recovery", _params, %{assigns: %{plan: plan}} = socket)
      when is_map(plan) do
    case start_recovery(context(socket), plan.plan_id, plan.plan_hash) do
      {:ok, operation} ->
        {:noreply,
         socket
         |> assign(operation: operation, error: nil)
         |> push_patch(
           to:
             ~p"/recoveries?#{[target_id: socket.assigns.target_id, operation_id: operation.operation_id]}"
         )}

      {:error, failure} ->
        {:noreply, assign(socket, error: error_label(failure))}
    end
  end

  def handle_event("start_recovery", _params, socket), do: {:noreply, socket}

  def handle_event(
        "reconcile_recovery",
        _params,
        %{assigns: %{operation: %{operation_id: operation_id}}} = socket
      ) do
    case reconcile_recovery(context(socket), operation_id) do
      {:ok, operation} -> {:noreply, assign(socket, operation: operation, error: nil)}
      {:error, failure} -> {:noreply, assign(socket, error: error_label(failure))}
    end
  end

  def handle_event("reconcile_recovery", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <TargetRecoveryPage.page
      target_id={@target_id}
      plan={@plan}
      operation={@operation}
      planning?={@planning?}
      error={@error}
    />
    """
  end

  defp context(socket), do: socket.assigns.current_scope.operator_context

  defp plan_recovery(context, target_id, reason) do
    Application.get_env(
      :favn_view,
      :plan_operator_target_recovery_fun,
      &FavnOrchestrator.plan_operator_target_recovery/3
    ).(context, target_id, reason)
  end

  defp start_recovery(context, plan_id, plan_hash) do
    Application.get_env(
      :favn_view,
      :start_operator_target_recovery_fun,
      &FavnOrchestrator.start_operator_target_recovery/3
    ).(context, plan_id, plan_hash)
  end

  defp reconcile_recovery(context, operation_id) do
    Application.get_env(
      :favn_view,
      :reconcile_operator_target_recovery_fun,
      &FavnOrchestrator.reconcile_operator_target_recovery/2
    ).(context, operation_id)
  end

  defp load_operation(_context, nil), do: nil

  defp load_operation(context, operation_id) do
    Application.get_env(
      :favn_view,
      :get_operator_target_recovery_fun,
      &FavnOrchestrator.get_operator_target_recovery/2
    ).(context, operation_id)
    |> case do
      {:ok, operation} -> operation
      {:error, _failure} -> nil
    end
  end

  defp error_label(:forbidden), do: "Administrator access is required."
  defp error_label(_failure), do: "Recovery could not be completed. Review the evidence and logs."
end
