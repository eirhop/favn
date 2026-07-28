defmodule FavnView.Components.TargetRecoveryPage do
  @moduledoc "Operator surface for evidence-backed interrupted-target recovery."

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.GlassPanel

  attr :target_id, :string, default: ""
  attr :plan, :map, default: nil
  attr :operation, :map, default: nil
  attr :error, :string, default: nil
  attr :planning?, :boolean, default: false

  def page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Target recovery"
      subtitle="Restore proven Favn ownership after an interrupted initial materialization"
      nav_items={AssetCataloguePage.nav_items(:recoveries)}
    >
      <div
        class="mx-auto grid w-full max-w-[100rem] gap-5 xl:grid-cols-[minmax(22rem,0.8fr)_minmax(0,1.4fr)]"
        data-testid="target-recovery-page"
      >
        <GlassPanel.glass_panel class="p-5 sm:p-6">
          <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
            Plan recovery
          </p>
          <p class="mt-2 text-sm text-base-content/65">
            Planning is read-only. Favn will require its original generation,
            successful materialization, physical fingerprint, and marker identity.
          </p>

          <.form
            for={%{}}
            as={:recovery}
            phx-submit="plan_recovery"
            class="mt-5 space-y-4"
          >
            <label class="form-control block">
              <span class="label-text text-xs text-base-content/60">Target id</span>
              <input
                name="recovery[target_id]"
                value={@target_id}
                required
                class="input input-bordered mt-1 w-full bg-base-100/30"
                data-testid="recovery-target"
              />
            </label>
            <label class="form-control block">
              <span class="label-text text-xs text-base-content/60">Reason</span>
              <textarea
                name="recovery[reason]"
                required
                class="textarea textarea-bordered mt-1 min-h-24 w-full bg-base-100/30"
                data-testid="recovery-reason"
              >Interrupted initial materialization</textarea>
            </label>
            <button
              class="btn btn-primary w-full"
              disabled={@planning?}
              data-testid="plan-recovery"
            >
              {if @planning?, do: "Inspecting evidence…", else: "Create recovery plan"}
            </button>
          </.form>

          <p :if={@error} class="mt-4 text-sm text-error" data-testid="recovery-error">
            {@error}
          </p>
        </GlassPanel.glass_panel>

        <div class="space-y-5">
          <GlassPanel.glass_panel :if={@plan} class="p-5 sm:p-6" data-testid="recovery-plan">
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
                  Immutable evidence plan
                </p>
                <p class="mt-2 break-all font-mono text-xs">{field(@plan, :plan_id)}</p>
              </div>
              <button
                :if={field(field(@plan, :permissions, %{}), :start, false)}
                phx-click="start_recovery"
                class="btn btn-warning btn-sm"
                data-confirm="Activate this exact proven generation?"
                data-testid="start-recovery"
              >
                Start recovery
              </button>
            </div>
            <dl class="mt-5 grid gap-3 text-sm sm:grid-cols-2">
              <.item label="Plan hash" value={field(@plan, :plan_hash)} />
              <.item label="Expires" value={field(@plan, :expires_at)} />
              <.item label="Generation" value={payload(@plan, :target_generation_id)} />
              <.item label="Materialization" value={payload(@plan, :materialization_id)} />
              <.item label="Physical fingerprint" value={payload(@plan, :physical_fingerprint)} />
              <.item label="Source manifest" value={payload(@plan, :source_manifest_id)} />
            </dl>
          </GlassPanel.glass_panel>

          <GlassPanel.glass_panel
            :if={@operation}
            class="p-5 sm:p-6"
            data-testid="recovery-operation"
          >
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p class="text-xs font-semibold uppercase tracking-[0.18em] text-base-content/50">
                  Recovery operation
                </p>
                <p class="mt-2 break-all font-mono text-xs">
                  {field(@operation, :operation_id)}
                </p>
              </div>
              <button
                :if={field(field(@operation, :permissions, %{}), :reconcile, false)}
                phx-click="reconcile_recovery"
                class="btn btn-outline btn-sm"
                data-testid="reconcile-recovery"
              >
                Reconcile marker
              </button>
            </div>
            <dl class="mt-5 grid gap-3 text-sm sm:grid-cols-2">
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
          </GlassPanel.glass_panel>

          <GlassPanel.glass_panel :if={!@plan && !@operation} class="p-8 text-center">
            <p class="text-sm text-base-content/60">
              No recovery has been planned. Arbitrary tables cannot be adopted from this page.
            </p>
          </GlassPanel.glass_panel>
        </div>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, default: nil

  defp item(assigns) do
    ~H"""
    <div :if={not is_nil(@value)}>
      <dt class="text-xs text-base-content/45">{@label}</dt>
      <dd class="mt-1 break-all font-mono text-xs">{format_value(@value)}</dd>
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
