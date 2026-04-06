defmodule Favn.Triggers.Schedules do
  @moduledoc """
  Reusable named schedule trigger definitions.

  Modules can declare repeated top-level `schedule/2` clauses:

      defmodule MyApp.Schedules do
        use Favn.Triggers.Schedules

        schedule :daily,
          cron: "0 2 * * *",
          timezone: "Europe/Oslo",
          missed: :skip,
          overlap: :forbid
      end
  """

  alias Favn.Triggers.Schedule

  @type fetch_error ::
          :not_schedule_module | :schedule_not_defined | {:schedule_not_found, atom()}

  defmacro __using__(_opts) do
    quote do
      import Favn.Triggers.Schedules

      Module.register_attribute(__MODULE__, :favn_named_schedules, accumulate: true)

      @before_compile Favn.Triggers.Schedules
    end
  end

  defmacro schedule(name, opts) do
    schedule =
      case Schedule.named(name, opts) do
        {:ok, value} ->
          value

        {:error, reason} ->
          raise ArgumentError,
                "invalid schedule declaration #{inspect(name)}: #{inspect(reason)}"
      end

    quote bind_quoted: [name: name, schedule: Macro.escape(schedule)] do
      unless is_atom(name) do
        raise ArgumentError, "schedule name must be an atom"
      end

      existing = Module.get_attribute(__MODULE__, :favn_named_schedules) || []

      if Enum.any?(existing, fn {existing_name, _schedule} -> existing_name == name end) do
        raise ArgumentError,
              "schedule #{inspect(name)} is already declared in #{inspect(__MODULE__)}"
      end

      Module.put_attribute(__MODULE__, :favn_named_schedules, {name, schedule})
    end
  end

  defmacro __before_compile__(env) do
    schedules =
      env.module
      |> Module.get_attribute(:favn_named_schedules)
      |> Enum.reverse()
      |> Map.new()

    quote do
      @doc false
      @spec __favn_schedules__() :: %{optional(atom()) => Favn.Triggers.Schedule.compile_t()}
      def __favn_schedules__, do: unquote(Macro.escape(schedules))

      @doc false
      @spec __favn_schedule__(atom()) ::
              {:ok, Favn.Triggers.Schedule.compile_t()} | {:error, :not_found}
      def __favn_schedule__(name) when is_atom(name) do
        case Map.fetch(unquote(Macro.escape(schedules)), name) do
          {:ok, schedule} -> {:ok, schedule}
          :error -> {:error, :not_found}
        end
      end
    end
  end

  @spec fetch(module(), atom()) :: {:ok, Schedule.t()} | {:error, fetch_error()}
  def fetch(module, name) when is_atom(module) and is_atom(name) do
    if function_exported?(module, :__favn_schedule__, 1) do
      case module.__favn_schedule__(name) do
        {:ok, %Schedule{} = schedule} -> {:ok, Schedule.apply_ref(schedule, {module, name})}
        {:error, :not_found} -> {:error, {:schedule_not_found, name}}
        _other -> {:error, :schedule_not_defined}
      end
    else
      {:error, :not_schedule_module}
    end
  rescue
    _error -> {:error, :schedule_not_defined}
  end

  def fetch(_module, _name), do: {:error, :not_schedule_module}
end
