defmodule FavnView.Components.ScheduleUi do
  @moduledoc """
  Badges and tables shared by the schedule list and schedule detail.

  A schedule has two independent states, and an operator reads them together:
  whether the definition is *allowed* to run (activation) and whether it *is*
  running (runtime). They are separate badges for that reason, and both go
  through `FavnView.UI.Badge` so a neutral state is legible on the dark theme —
  DaisyUI's own `badge-neutral` paints its text in the theme's neutral surface
  colour, which on a dark surface is very nearly the background.

  Activation tone is domain knowledge, not shared vocabulary: a disabled
  schedule is a problem worth colouring like one, while `:disabled` elsewhere in
  the interface only means a control cannot be used. So the mapping lives here.
  """

  use FavnView, :html

  @doc """
  Whether one schedule definition is allowed to submit runs.

  ## Examples

      <.activation_badge state={:enabled} label="Enabled" />
  """
  attr :state, :atom, required: true
  attr :label, :string, required: true

  def activation_badge(assigns) do
    ~H"""
    <.status_badge tone={activation_tone(@state)} label={@label} />
    """
  end

  @doc """
  What the scheduler is doing with one schedule right now.

  ## Examples

      <.runtime_badge state={:running} label="Running" />
  """
  attr :state, :atom, required: true
  attr :label, :string, required: true

  def runtime_badge(assigns) do
    ~H"""
    <.status_badge tone={runtime_tone(@state)} label={@label} />
    """
  end

  @doc """
  The scheduler's last failure for one schedule, or a dash when there is none.

  The phase is the label and the message is the tooltip: an operator scanning a
  list needs to know which phase broke, and only then what it said.
  """
  attr :error, :map, default: nil

  def scheduler_error_badge(assigns) do
    ~H"""
    <span :if={!@error}>-</span>
    <.badge :if={@error} tone={:warning} icon="hero-exclamation-triangle" title={@error.message}>
      {@error.phase_label}
    </.badge>
    """
  end

  @doc """
  What a previewed occurrence would do when it comes due.

  ## Examples

      <.occurrence_status_badge status={:upcoming} label="Upcoming" />
  """
  attr :status, :atom, required: true
  attr :label, :string, required: true

  def occurrence_status_badge(assigns) do
    ~H"""
    <.badge tone={occurrence_tone(@status)}>{@label}</.badge>
    """
  end

  @doc """
  Bounded future occurrences, as the scheduler would evaluate them.

  A preview submits nothing, so the notes column carries the reason an
  occurrence would not run — that is the column an operator came for.
  """
  attr :occurrences, :list, required: true

  def occurrence_preview_table(assigns) do
    ~H"""
    <.data_table
      id="schedule-occurrence-table"
      rows={@occurrences}
      row_testid="schedule-occurrence-row"
      data-testid="schedule-occurrence-table"
    >
      <:col :let={occurrence} label="Due at" class="whitespace-nowrap">
        <p class="font-medium text-base-content">{occurrence.due_label}</p>

        <p class="text-sm favn-text-subtle">{occurrence.timezone}</p>
      </:col>

      <:col :let={occurrence} label="Window" class="min-w-48 text-sm favn-text-muted">
        {occurrence.window_label}
      </:col>

      <:col :let={occurrence} label="Status">
        <.occurrence_status_badge status={occurrence.status} label={occurrence.status_label} />
      </:col>

      <:col :let={occurrence} label="Notes" class="min-w-48 text-sm favn-text-muted">
        <span :if={occurrence.notes == []}>-</span>
        <span :for={note <- occurrence.notes} class="mr-2 inline-block">{note}</span>
      </:col>
    </.data_table>
    """
  end

  @doc """
  Tone for one activation state.

  ## Examples

      iex> FavnView.Components.ScheduleUi.activation_tone(:disabled)
      :error

      iex> FavnView.Components.ScheduleUi.activation_tone(:retired)
      :neutral
  """
  @spec activation_tone(atom()) :: atom()
  def activation_tone(:enabled), do: :success
  def activation_tone(:pending_activation), do: :warning
  def activation_tone(:needs_review), do: :warning
  def activation_tone(:disabled), do: :error
  def activation_tone(_state), do: :neutral

  @doc """
  Tone for one runtime state.

  ## Examples

      iex> FavnView.Components.ScheduleUi.runtime_tone(:running)
      :info

      iex> FavnView.Components.ScheduleUi.runtime_tone(:inactive)
      :neutral
  """
  @spec runtime_tone(atom()) :: atom()
  def runtime_tone(:running), do: :info
  def runtime_tone(:queued), do: :warning
  def runtime_tone(_state), do: :neutral

  @doc """
  Tone for one previewed occurrence status.

  ## Examples

      iex> FavnView.Components.ScheduleUi.occurrence_tone(:blocked)
      :error
  """
  @spec occurrence_tone(atom()) :: atom()
  def occurrence_tone(:upcoming), do: :info
  def occurrence_tone(:running), do: :info
  def occurrence_tone(:queued), do: :warning
  def occurrence_tone(:blocked), do: :error
  def occurrence_tone(_status), do: :neutral
end
