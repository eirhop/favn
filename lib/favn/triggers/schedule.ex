defmodule Favn.Triggers.Schedule do
  @moduledoc """
  Normalized schedule trigger definition used by pipeline resolution/runtime context.

  Cron is currently the only supported trigger kind.
  """

  @type kind :: :cron
  @type missed_policy :: :skip | :one | :all
  @type overlap_policy :: :forbid | :allow | :queue_one
  @type ref :: {module(), atom()}

  @enforce_keys [:kind, :cron, :timezone, :missed, :overlap, :origin]
  defstruct id: nil,
            ref: nil,
            kind: :cron,
            cron: nil,
            timezone: nil,
            timezone_source: :schedule,
            missed: :skip,
            overlap: :forbid,
            origin: :inline

  @type t :: %__MODULE__{
          id: atom() | nil,
          ref: ref() | nil,
          kind: kind(),
          cron: String.t(),
          timezone: String.t(),
          timezone_source: :schedule | :app_default,
          missed: missed_policy(),
          overlap: overlap_policy(),
          origin: :inline | :named
        }

  @type compile_t :: %__MODULE__{timezone: String.t() | nil}

  @spec new_inline(keyword()) :: {:ok, compile_t()} | {:error, term()}
  def new_inline(opts) when is_list(opts) do
    with :ok <- validate_supported_opts(opts),
         {:ok, cron} <- fetch_cron(opts),
         {:ok, timezone} <- validate_timezone(Keyword.get(opts, :timezone)),
         {:ok, missed} <- validate_missed(Keyword.get(opts, :missed, :skip)),
         {:ok, overlap} <- validate_overlap(Keyword.get(opts, :overlap, :forbid)) do
      {:ok,
       %__MODULE__{
         kind: :cron,
         cron: cron,
         timezone: timezone,
         missed: missed,
         overlap: overlap,
         origin: :inline,
         timezone_source: if(timezone == nil, do: :app_default, else: :schedule)
       }}
    end
  end

  def new_inline(_invalid), do: {:error, :invalid_schedule}

  @spec named(atom(), keyword()) :: {:ok, compile_t()} | {:error, term()}
  def named(name, opts) when is_atom(name) and is_list(opts) do
    with {:ok, schedule} <- new_inline(opts) do
      {:ok, %{schedule | id: name, origin: :named}}
    end
  end

  def named(_name, _opts), do: {:error, :invalid_schedule}

  @spec apply_ref(compile_t(), ref()) :: t()
  def apply_ref(%__MODULE__{} = schedule, {module, name}) do
    %{schedule | id: schedule.id || name, ref: {module, name}, origin: :named}
  end

  @spec apply_default_timezone(compile_t(), String.t() | nil) :: {:ok, t()} | {:error, term()}
  def apply_default_timezone(%__MODULE__{} = schedule, default_timezone) do
    case schedule.timezone do
      timezone when is_binary(timezone) ->
        {:ok, %{schedule | timezone_source: :schedule}}

      nil ->
        with {:ok, effective} <- validate_timezone(default_timezone || "Etc/UTC") do
          {:ok, %{schedule | timezone: effective, timezone_source: :app_default}}
        end
    end
  end

  @spec default_timezone() :: String.t()
  def default_timezone do
    case Application.get_env(:favn, :scheduler, []) do
      scheduler when is_list(scheduler) -> Keyword.get(scheduler, :default_timezone, "Etc/UTC")
      _ -> "Etc/UTC"
    end
  end

  @spec validate_supported_opts(keyword()) :: :ok | {:error, term()}
  def validate_supported_opts(opts) when is_list(opts) do
    supported = [:cron, :timezone, :missed, :overlap]

    case Keyword.keys(opts) -- supported do
      [] -> :ok
      unknown -> {:error, {:unsupported_schedule_opts, unknown}}
    end
  end

  defp fetch_cron(opts) do
    case Keyword.fetch(opts, :cron) do
      {:ok, cron} when is_binary(cron) ->
        if byte_size(String.trim(cron)) > 0 do
          {:ok, cron}
        else
          {:error, {:invalid_schedule_cron, cron}}
        end

      {:ok, _invalid} ->
        {:error, {:invalid_schedule_cron, Keyword.get(opts, :cron)}}

      :error ->
        {:error, :missing_schedule_cron}
    end
  end

  defp validate_timezone(nil), do: {:ok, nil}

  defp validate_timezone(timezone) when is_binary(timezone) do
    if String.trim(timezone) == "" do
      {:error, {:invalid_schedule_timezone, timezone}}
    else
      {:ok, timezone}
    end
  end

  defp validate_timezone(other), do: {:error, {:invalid_schedule_timezone, other}}

  defp validate_missed(value) when value in [:skip, :one, :all], do: {:ok, value}
  defp validate_missed(other), do: {:error, {:invalid_schedule_missed, other}}

  defp validate_overlap(value) when value in [:forbid, :allow, :queue_one], do: {:ok, value}
  defp validate_overlap(other), do: {:error, {:invalid_schedule_overlap, other}}
end
