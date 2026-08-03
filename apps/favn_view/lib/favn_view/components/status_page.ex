defmodule FavnView.Components.StatusPage do
  @moduledoc """
  The status screen: what needs the operator, and nothing else.

  Every other screen answers "what exists". This one answers "what is wrong and
  what do I do about it", which is the question an operator actually arrives
  with. It is the root route for that reason.

  ## What belongs here

  A row on this page is a *concern*: something an operator would act on, stated
  in their language, with the one action that addresses it. A count with no next
  step is not a concern and does not belong here — that is what the catalogue
  pages are for.

  Concerns are grouped by kind so the page can be scanned, and groups are
  ordered by severity: failures before staleness before pending work. A group
  with nothing in it is not rendered at all rather than rendered empty, because
  an empty group still costs a screenful of scrolling to skip.

  ## Partial failure is not failure

  Concerns come from four independent sources. If one is unavailable the rest
  still render, and the page says which source it could not reach. A broken
  schedule backend must not hide a failing run.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell

  @type concern :: %{
          required(:id) => String.t(),
          required(:title) => String.t(),
          required(:detail) => String.t(),
          required(:action_label) => String.t(),
          required(:action_path) => String.t(),
          optional(:meta) => String.t() | nil,
          optional(:tone) => atom()
        }

  @type group :: %{
          required(:id) => atom(),
          required(:title) => String.t(),
          required(:description) => String.t(),
          required(:icon) => String.t(),
          required(:tone) => atom(),
          required(:concerns) => [concern()]
        }

  attr :groups, :list, required: true, doc: "concern groups, most severe first; empty means clear"
  attr :unavailable, :list, default: [], doc: "sources that could not be reached"
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :flash, :map, default: %{}

  def status_page(assigns) do
    assigns = assign(assigns, :total, total_concerns(assigns.groups))

    ~H"""
    <AppShell.app_shell
      title="Status"
      subtitle={subtitle(@total)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    >
      <div
        class="mx-auto flex w-full max-w-[80rem] flex-1 flex-col pb-24 lg:pb-0"
        data-testid="status-page"
      >
        <.loading_state :if={@loading} label="Checking for anything that needs attention" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load status"
          description={@error}
          data-testid="status-error-state"
        />
        <.stack :if={!@loading && !@error} gap={{:md, :lg}}>
          <.notice :for={source <- @unavailable} tone={:warning}>
            {source} could not be reached, so anything needing attention there is not listed.
          </.notice>

          <.empty_state
            :if={@groups == []}
            title="Nothing needs you"
            description="No failing runs, no stale assets, no schedules in error, and no operations waiting to be reconciled."
            icon="hero-check-circle"
            data-testid="status-all-clear"
          /> <.concern_group :for={group <- @groups} group={group} />
        </.stack>
      </div>
    </AppShell.app_shell>
    """
  end

  attr :group, :map, required: true

  defp concern_group(assigns) do
    ~H"""
    <.panel data-testid={"status-group-#{@group.id}"}>
      <:header title={@group.title} subtitle={@group.description} icon={@group.icon} />
      <.stack gap={:sm}>
        <.concern_row :for={concern <- @group.concerns} concern={concern} tone={@group.tone} />
      </.stack>
    </.panel>
    """
  end

  attr :concern, :map, required: true
  attr :tone, :atom, required: true

  defp concern_row(assigns) do
    ~H"""
    <.list_card
      id={"concern-#{@concern.id}"}
      navigate={@concern.action_path}
      data-testid="status-concern"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0 space-y-1">
          <.inline gap={:sm}>
            <.status_dot tone={Map.get(@concern, :tone, @tone)} label={@concern.title} />
            <span class="truncate font-medium">{@concern.title}</span>
          </.inline>

          <p class="favn-text-muted text-sm">{@concern.detail}</p>
        </div>

        <div class="flex shrink-0 items-center gap-2">
          <.meta :if={@concern[:meta]}>{@concern.meta}</.meta>
          <span class="favn-text-subtle text-sm">{@concern.action_label}</span>
          <.icon name="hero-chevron-right" size={:sm} class="favn-text-subtle" />
        </div>
      </div>
    </.list_card>
    """
  end

  @doc """
  The number of concerns across every group.
  """
  @spec total_concerns([group()]) :: non_neg_integer()
  def total_concerns(groups), do: Enum.sum_by(groups, &length(&1.concerns))

  defp subtitle(0), do: "Nothing needs attention"
  defp subtitle(1), do: "1 thing needs attention"
  defp subtitle(total), do: "#{total} things need attention"
end
