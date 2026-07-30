defmodule FavnView.Components.TargetRecoveryPage do
  @moduledoc """
  Operator surface for evidence-backed interrupted-target recovery.

  Planning is read-only. The orchestrator requires the original generation, a
  successful materialization, a matching physical fingerprint, and marker
  identity before it will offer recovery, so this page never adopts a table on
  the operator's behalf — it renders the evidence the backend produced and, only
  when the backend grants the permission, the control that acts on it.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation

  attr :target_id, :string, default: ""
  attr :reason, :string, default: "Interrupted initial materialization"
  attr :plan, :map, default: nil
  attr :operation, :map, default: nil
  attr :error, :string, default: nil
  attr :planning?, :boolean, default: false
  attr :flash, :map, default: %{}

  def page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Target recovery"
      subtitle="Restore proven Favn ownership after an interrupted initial materialization"
      nav_items={Navigation.items(:recoveries)}
      flash={@flash}
    >
      <div
        class="mx-auto grid w-full max-w-[100rem] gap-5 xl:grid-cols-[minmax(22rem,0.8fr)_minmax(0,1.4fr)]"
        data-testid="target-recovery-page"
      >
        <.plan_form
          target_id={@target_id}
          reason={@reason}
          error={@error}
          planning?={@planning?}
        />
        <div class="space-y-5">
          <.plan_panel :if={@plan} plan={@plan} />
          <.operation_panel :if={@operation} operation={@operation} />
          <.empty_state
            :if={!@plan && !@operation}
            title="No recovery planned"
            description="Arbitrary tables cannot be adopted from this page. Plan a recovery to see the evidence Favn requires."
            icon="hero-shield-check"
          />
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  @doc """
  The read-only planning form.
  """
  attr :target_id, :string, required: true
  attr :reason, :string, required: true
  attr :error, :string, default: nil
  attr :planning?, :boolean, default: false

  def plan_form(assigns) do
    ~H"""
    <.panel>
      <:header title="Plan recovery" />
      <p class="text-sm favn-text-muted">
        Planning is read-only. Favn will require its original generation, successful
        materialization, physical fingerprint, and marker identity.
      </p>

      <.form
        for={%{}}
        as={:recovery}
        phx-submit="plan_recovery"
        class="mt-5 space-y-4"
        data-command-operation="target_recovery_plan"
        data-command-resource-field="recovery[target_id]"
      >
        <.input
          name="recovery[target_id]"
          label="Target id"
          value={@target_id}
          required
          data-testid="recovery-target"
        />
        <.input
          name="recovery[reason]"
          type="textarea"
          label="Reason"
          value={@reason}
          required
          data-testid="recovery-reason"
        />
        <.button
          type="submit"
          block
          loading={@planning?}
          disabled={@planning?}
          data-testid="plan-recovery"
        >
          {if @planning?, do: "Inspecting evidence", else: "Create recovery plan"}
        </.button>
      </.form>

      <.notice :if={@error} tone={:error} class="mt-4" data-testid="recovery-error">
        {@error}
      </.notice>
    </.panel>
    """
  end

  @doc """
  The immutable evidence plan the orchestrator produced.
  """
  attr :plan, :map, required: true

  def plan_panel(assigns) do
    ~H"""
    <.panel data-testid="recovery-plan">
      <:header title="Immutable evidence plan" subtitle={field(@plan, :plan_id)} />
      <:actions>
        <.button
          :if={field(field(@plan, :permissions, %{}), :start, false)}
          variant={:danger}
          phx-click="start_recovery"
          data-command-operation="target_recovery_start"
          data-command-resource={field(@plan, :plan_id)}
          data-confirm="Activate this exact proven generation?"
          data-testid="start-recovery"
        >
          Start recovery
        </.button>
      </:actions>

      <dl class="grid gap-3 text-sm sm:grid-cols-2">
        <.item label="Plan hash" value={field(@plan, :plan_hash)} />
        <.item label="Expires" value={field(@plan, :expires_at)} />
        <.item label="Generation" value={payload(@plan, :target_generation_id)} />
        <.item label="Materialization" value={payload(@plan, :materialization_id)} />
        <.item label="Physical fingerprint" value={payload(@plan, :physical_fingerprint)} />
        <.item label="Source manifest" value={payload(@plan, :source_manifest_id)} />
      </dl>
    </.panel>
    """
  end

  @doc """
  The recovery operation the orchestrator is tracking.
  """
  attr :operation, :map, required: true

  def operation_panel(assigns) do
    ~H"""
    <.panel data-testid="recovery-operation">
      <:header title="Recovery operation" subtitle={field(@operation, :operation_id)} />
      <:actions>
        <.button
          :if={field(field(@operation, :permissions, %{}), :reconcile, false)}
          variant={:secondary}
          phx-click="reconcile_recovery"
          data-command-operation="target_recovery_reconcile"
          data-command-resource={field(@operation, :operation_id)}
          data-testid="reconcile-recovery"
        >
          Reconcile marker
        </.button>
      </:actions>

      <dl class="grid gap-3 text-sm sm:grid-cols-2">
        <.item label="State" value={field(@operation, :state)} />
        <.item label="Phase" value={field(@operation, :phase)} />
        <.item label="Target" value={field(@operation, :target_id)} />
        <.item label="Generation" value={field(@operation, :target_generation_id)} />
        <.item
          label="Compatibility"
          value={field(field(@operation, :compatibility_result, %{}), :status)}
        />
        <.item
          label="Unknown outcome"
          value={field(field(@operation, :unknown_outcome, %{}), :reason_code)}
        />
      </dl>
    </.panel>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, default: nil

  defp item(assigns) do
    ~H"""
    <div :if={not is_nil(@value)}>
      <dt class="text-xs favn-text-subtle">{@label}</dt>

      <dd class="mt-1"><.mono value={format_value(@value)} /></dd>
    </div>
    """
  end

  defp payload(plan, key), do: plan |> field(:payload, %{}) |> field(key)
  defp format_value(value) when is_atom(value), do: Atom.to_string(value)
  defp format_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp format_value(value), do: value

  defp field(map, key, default \\ nil)

  defp field(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp field(_value, _key, default), do: default
end
