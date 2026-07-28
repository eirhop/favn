defmodule FavnView.TargetRecoveryLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.TargetRecoveryPage

  @impl true
  def mount(params, _session, socket) do
    operation = load_operation(context(socket), Map.get(params, "operation_id"))

    attempt =
      case operation do
        nil -> planning_attempt_from_params(params)
        operation -> planning_attempt(operation)
      end

    {:ok,
     assign(socket,
       target_id: Map.get(params, "target_id", field(operation, :target_id, "")),
       reason: field(operation, :reason, "Interrupted initial materialization"),
       plan: nil,
       operation: operation,
       planning_attempt: attempt,
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
    attempt = planning_attempt(socket.assigns.planning_attempt, target_id, reason)

    socket =
      assign(socket,
        planning_attempt: attempt,
        planning?: true,
        error: nil,
        plan: nil,
        target_id: target_id,
        reason: reason
      )

    case plan_recovery(context(socket), target_id, reason, attempt) do
      {:ok, plan} ->
        {:noreply,
         assign(socket, planning_attempt: nil, planning?: false, plan: plan, operation: nil)}

      {:error, failure} ->
        {:noreply, planning_failed(socket, attempt, failure)}
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
      reason={@reason}
      plan={@plan}
      operation={@operation}
      planning?={@planning?}
      error={@error}
    />
    """
  end

  defp context(socket), do: socket.assigns.current_scope.operator_context

  defp plan_recovery(context, target_id, reason, attempt) do
    Application.get_env(
      :favn_view,
      :plan_operator_target_recovery_fun,
      &FavnOrchestrator.plan_operator_target_recovery/4
    ).(
      context,
      target_id,
      reason,
      operation_id: attempt.operation_id,
      idempotency_key: attempt.operation_id
    )
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
    case get_operation(context, operation_id) do
      {:ok, operation} -> operation
      {:error, _failure} -> nil
    end
  end

  defp get_operation(context, operation_id) do
    Application.get_env(
      :favn_view,
      :get_operator_target_recovery_fun,
      &FavnOrchestrator.get_operator_target_recovery/2
    ).(context, operation_id)
  end

  defp planning_failed(socket, attempt, failure) do
    base = assign(socket, planning?: false, error: error_label(failure))

    case get_operation(context(socket), attempt.operation_id) do
      {:ok, operation} ->
        if planning_operation?(operation) do
          base
          |> assign(planning_attempt: attempt, operation: operation)
          |> patch_planning_attempt(attempt)
        else
          assign(base, planning_attempt: nil, operation: operation)
        end

      {:error, _lookup_failure} ->
        base
        |> assign(planning_attempt: attempt)
        |> patch_planning_attempt(attempt)
    end
  end

  defp planning_attempt(operation) do
    if planning_operation?(operation) do
      target_id = field(operation, :target_id)
      reason = field(operation, :reason)

      %{
        operation_id: field(operation, :operation_id),
        target_id: target_id,
        request_fingerprint: planning_request_fingerprint(target_id, reason)
      }
    end
  end

  defp planning_attempt_from_params(params) when is_map(params) do
    operation_id = Map.get(params, "operation_id")
    target_id = Map.get(params, "target_id")
    request_fingerprint = Map.get(params, "attempt")

    if valid_attempt_param?(operation_id, 255) and valid_attempt_param?(target_id, 255) and
         valid_attempt_param?(request_fingerprint, 64) do
      %{
        operation_id: operation_id,
        target_id: target_id,
        request_fingerprint: request_fingerprint
      }
    end
  end

  defp planning_attempt(
         %{target_id: target_id, request_fingerprint: request_fingerprint} = attempt,
         target_id,
         reason
       ) do
    if request_fingerprint == planning_request_fingerprint(target_id, reason),
      do: attempt,
      else: new_planning_attempt(target_id, reason)
  end

  defp planning_attempt(_attempt, target_id, reason),
    do: new_planning_attempt(target_id, reason)

  defp new_planning_attempt(target_id, reason) do
    %{
      operation_id:
        "target_recovery_ui_" <>
          Base.encode16(:crypto.strong_rand_bytes(16), case: :lower),
      target_id: target_id,
      request_fingerprint: planning_request_fingerprint(target_id, reason)
    }
  end

  defp patch_planning_attempt(socket, attempt) do
    push_patch(socket,
      to:
        ~p"/recoveries?#{[target_id: attempt.target_id, operation_id: attempt.operation_id, attempt: attempt.request_fingerprint]}"
    )
  end

  defp planning_request_fingerprint(target_id, reason) do
    ["target_recovery_ui", target_id, reason]
    |> Enum.intersperse(<<0>>)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp valid_attempt_param?(value, max_bytes),
    do: is_binary(value) and byte_size(value) in 1..max_bytes

  defp planning_operation?(operation),
    do: field(operation, :state) in [:planning, "planning"]

  defp field(map, key, default \\ nil)

  defp field(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp field(_value, _key, default), do: default

  defp error_label(:forbidden), do: "Administrator access is required."
  defp error_label(_failure), do: "Recovery could not be completed. Review the evidence and logs."
end
