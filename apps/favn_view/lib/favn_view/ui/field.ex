defmodule FavnView.UI.Field do
  @moduledoc """
  Form and filter controls.

  Favn has two kinds of input. *Filters* narrow what is already on screen and
  submit on change; *fields* collect a value that the operator submits
  deliberately. They look related but behave differently, so they are separate
  components.

  | Component | Use it for |
  | --- | --- |
  | `filter_bar/1` | the row of controls above a list |
  | `search_field/1` | free-text narrowing, debounced |
  | `select_field/1` | narrowing by a bounded set |
  | `input/1` | a real form field with a label and errors |

  Filter controls carry no visible label. Their placeholder is not a label, so
  each one also renders a screen-reader-only name.

  ## Examples

      <.filter_bar on_change="filter_assets">
        <.search_field name="filters[search]" value={@filters.search} label="Search assets" />
        <.select_field name="filters[connection]" label="Connection" options={@connections} value={@filters.connection} icon="hero-circle-stack" />
      </.filter_bar>
  """

  use Phoenix.Component
  use Gettext, backend: FavnView.Gettext

  import FavnView.UI.Icon

  @doc """
  The control row above a list.

  `on_change` is pushed on every change and on submit, so filtering works with
  and without JavaScript.
  """
  attr :on_change, :string, required: true, doc: "the LiveView event name"
  attr :id, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def filter_bar(assigns) do
    ~H"""
    <form
      id={@id}
      phx-change={@on_change}
      phx-submit={@on_change}
      class={["grid grid-cols-2 gap-2.5 lg:grid-cols-[1fr_12rem_12rem] lg:gap-3", @class]}
      {@rest}
    >
      {render_slot(@inner_block)}
    </form>
    """
  end

  attr :name, :string, required: true
  attr :label, :string, required: true, doc: "screen-reader name; also the default placeholder"
  attr :value, :string, default: nil
  attr :placeholder, :string, default: nil
  attr :id, :string, default: nil
  attr :debounce, :integer, default: 200
  attr :class, :any, default: nil

  def search_field(assigns) do
    ~H"""
    <label class={
      [
        "input input-sm favn-surface-control col-span-2 w-full gap-3 px-4",
        # Every control on a list toolbar shares the scope rail's 36px box, so the
        # row has one baseline rather than three.
        "h-9 min-h-9",
        "focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary lg:col-span-1",
        @class
      ]
    }>
      <.icon name="hero-magnifying-glass" size={:md} class="shrink-0 favn-text-muted" />
      <span class="sr-only">{@label}</span>
      <input
        id={@id}
        type="search"
        name={@name}
        value={@value}
        placeholder={@placeholder || @label}
        autocomplete="off"
        phx-debounce={@debounce}
      />
    </label>
    """
  end

  attr :name, :string, required: true
  attr :label, :string, required: true, doc: "screen-reader name for the control"
  attr :options, :list, required: true, doc: "`{label, value}` pairs"
  attr :value, :any, default: nil
  attr :icon, :string, default: nil
  attr :id, :string, default: nil
  attr :class, :any, default: nil

  def select_field(assigns) do
    ~H"""
    <label class={[
      "select select-sm favn-surface-control w-full gap-2 px-3",
      "h-9 min-h-9",
      "focus-within:outline focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-primary",
      @class
    ]}>
      <.icon :if={@icon} name={@icon} size={:md} class="shrink-0 favn-text-muted" />
      <span class="sr-only">{@label}</span>
      <select id={@id} name={@name} aria-label={@label} class="appearance-none">
        <option :for={{label, value} <- @options} value={value} selected={@value == value}>
          {label}
        </option>
      </select>
      <.icon name="hero-chevron-down" size={:md} class="shrink-0 favn-text-muted" />
    </label>
    """
  end

  @doc """
  A labelled form input with error display.

  A `Phoenix.HTML.FormField` may be passed as `field`, which supplies the name,
  id, value, and errors. Otherwise pass them explicitly.

  Supported types are every HTML input type plus `"select"` and `"textarea"`.
  `type="checkbox"` renders a boolean.

  ## Examples

      <.input field={@form[:username]} label="Username" autocomplete="username" />
      <.input field={@form[:role]} type="select" label="Role" options={[{"Admin", "admin"}]} />
  """
  attr :id, :any, default: nil
  attr :name, :any
  attr :label, :string, default: nil
  attr :value, :any

  attr :type, :string,
    default: "text",
    values: ~w(checkbox color date datetime-local email file month number password
               search select tel text textarea time url week hidden)

  attr :field, Phoenix.HTML.FormField, doc: "a form field struct, for example `@form[:email]`"

  attr :errors, :list, default: []
  attr :checked, :boolean, doc: "the checked flag for checkbox inputs"
  attr :prompt, :string, default: nil, doc: "the prompt for select inputs"
  attr :options, :list, doc: "options passed to `Phoenix.HTML.Form.options_for_select/2`"
  attr :multiple, :boolean, default: false
  attr :class, :any, default: nil
  attr :error_class, :any, default: nil

  attr :rest, :global,
    include: ~w(accept autocomplete capture cols disabled form list max maxlength min minlength
                multiple pattern placeholder readonly required rows size step)

  def input(%{field: %Phoenix.HTML.FormField{} = field} = assigns) do
    errors = if Phoenix.Component.used_input?(field), do: field.errors, else: []

    assigns
    |> assign(field: nil, id: assigns.id || field.id)
    |> assign(:errors, Enum.map(errors, &translate_error(&1)))
    |> assign_new(:name, fn -> if assigns.multiple, do: field.name <> "[]", else: field.name end)
    |> assign_new(:value, fn -> field.value end)
    |> input()
  end

  def input(%{type: "hidden"} = assigns) do
    ~H"""
    <input type="hidden" id={@id} name={@name} value={@value} {@rest} />
    """
  end

  def input(%{type: "checkbox"} = assigns) do
    assigns =
      assign_new(assigns, :checked, fn ->
        Phoenix.HTML.Form.normalize_value("checkbox", assigns[:value])
      end)

    ~H"""
    <div class="fieldset mb-2">
      <label for={@id}>
        <input
          type="hidden"
          name={@name}
          value="false"
          disabled={@rest[:disabled]}
          form={@rest[:form]}
        />
        <span class="label">
          <input
            type="checkbox"
            id={@id}
            name={@name}
            value="true"
            checked={@checked}
            class={@class || "checkbox checkbox-sm"}
            {@rest}
          />{@label}
        </span>
      </label>

      <.field_error :for={msg <- @errors}>{msg}</.field_error>
    </div>
    """
  end

  def input(%{type: "select"} = assigns) do
    ~H"""
    <div class="fieldset mb-2">
      <label for={@id}>
        <span :if={@label} class="label mb-1">{@label}</span>
        <select
          id={@id}
          name={@name}
          class={[@class || "w-full select", @errors != [] && (@error_class || "select-error")]}
          multiple={@multiple}
          {@rest}
        >
          <option :if={@prompt} value="">{@prompt}</option>
          {Phoenix.HTML.Form.options_for_select(@options, @value)}
        </select>
      </label>

      <.field_error :for={msg <- @errors}>{msg}</.field_error>
    </div>
    """
  end

  def input(%{type: "textarea"} = assigns) do
    ~H"""
    <div class="fieldset mb-2">
      <label for={@id}>
        <span :if={@label} class="label mb-1">{@label}</span> <textarea
          id={@id}
          name={@name}
          class={[@class || "w-full textarea", @errors != [] && (@error_class || "textarea-error")]}
          {@rest}
        >{Phoenix.HTML.Form.normalize_value("textarea", @value)}</textarea>
      </label>

      <.field_error :for={msg <- @errors}>{msg}</.field_error>
    </div>
    """
  end

  def input(assigns) do
    ~H"""
    <div class="fieldset mb-2">
      <label for={@id}>
        <span :if={@label} class="label mb-1">{@label}</span>
        <input
          type={@type}
          name={@name}
          id={@id}
          value={Phoenix.HTML.Form.normalize_value(@type, @value)}
          class={[@class || "w-full input", @errors != [] && (@error_class || "input-error")]}
          {@rest}
        />
      </label>

      <.field_error :for={msg <- @errors}>{msg}</.field_error>
    </div>
    """
  end

  @doc """
  Renders a validation message under a field.
  """
  slot :inner_block, required: true

  def field_error(assigns) do
    ~H"""
    <p class="mt-1.5 flex items-center gap-2 text-sm text-error">
      <.icon name="hero-exclamation-circle" size={:md} /> {render_slot(@inner_block)}
    </p>
    """
  end

  @doc """
  Translates a changeset error tuple using Gettext.
  """
  def translate_error({msg, opts}) do
    if count = opts[:count] do
      Gettext.dngettext(FavnView.Gettext, "errors", msg, msg, count, opts)
    else
      Gettext.dgettext(FavnView.Gettext, "errors", msg, opts)
    end
  end

  @doc """
  Translates the errors for one field from a keyword list of errors.
  """
  def translate_errors(errors, field) when is_list(errors) do
    for {^field, {msg, opts}} <- errors, do: translate_error({msg, opts})
  end
end
