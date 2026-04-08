defmodule Favn.Triggers.Schedule do
  @moduledoc """
  Normalized schedule trigger definition used by pipeline resolution/runtime context.

  Cron is currently the only supported trigger kind.
  """

  @type kind :: :cron
  @type missed_policy :: :skip | :one | :all
  @type overlap_policy :: :forbid | :allow | :queue_one
  @type ref :: {module(), atom()}

  @enforce_keys [:kind, :cron, :timezone, :missed, :overlap, :active, :origin]
  defstruct id: nil,
            ref: nil,
            kind: :cron,
            cron: nil,
            timezone: nil,
            timezone_source: :schedule,
            missed: :skip,
            overlap: :forbid,
            active: true,
            origin: :inline

  @type unresolved_t :: %__MODULE__{
          id: atom() | nil,
          ref: ref() | nil,
          kind: kind(),
          cron: String.t(),
          timezone: String.t() | nil,
          timezone_source: :schedule | :app_default,
          missed: missed_policy(),
          overlap: overlap_policy(),
          active: boolean(),
          origin: :inline | :named
        }

  @type t :: %__MODULE__{
          id: atom() | nil,
          ref: ref() | nil,
          kind: kind(),
          cron: String.t(),
          timezone: String.t(),
          timezone_source: :schedule | :app_default,
          missed: missed_policy(),
          overlap: overlap_policy(),
          active: boolean(),
          origin: :inline | :named
        }

  @spec new_inline(keyword()) :: {:ok, unresolved_t()} | {:error, term()}
  def new_inline(opts) when is_list(opts) do
    with :ok <- validate_keyword_opts(opts),
         :ok <- validate_duplicate_opts(opts),
         :ok <- validate_supported_opts(opts),
         {:ok, cron} <- fetch_cron(opts),
         {:ok, timezone} <- validate_timezone(Keyword.get(opts, :timezone)),
         {:ok, missed} <- validate_missed(Keyword.get(opts, :missed, :skip)),
         {:ok, overlap} <- validate_overlap(Keyword.get(opts, :overlap, :forbid)),
         {:ok, active} <- validate_active(Keyword.get(opts, :active, true)) do
      {:ok,
       %__MODULE__{
         kind: :cron,
         cron: cron,
         timezone: timezone,
         missed: missed,
         overlap: overlap,
         active: active,
         origin: :inline,
         timezone_source: if(timezone == nil, do: :app_default, else: :schedule)
       }}
    end
  end

  def new_inline(_invalid), do: {:error, :invalid_schedule}

  @spec named(atom(), keyword()) :: {:ok, unresolved_t()} | {:error, term()}
  def named(name, opts) when is_atom(name) and is_list(opts) do
    with {:ok, schedule} <- new_inline(opts) do
      {:ok, %{schedule | id: name, origin: :named}}
    end
  end

  def named(_name, _opts), do: {:error, :invalid_schedule}

  @spec apply_ref(unresolved_t(), ref()) :: unresolved_t()
  def apply_ref(%__MODULE__{} = schedule, {module, name}) do
    %{schedule | id: schedule.id || name, ref: {module, name}, origin: :named}
  end

  @spec apply_default_timezone(unresolved_t(), String.t() | nil) :: {:ok, t()} | {:error, term()}
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

  @spec validate_keyword_opts(term()) :: :ok | {:error, :invalid_schedule}
  def validate_keyword_opts(opts) when is_list(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, :invalid_schedule}
  end

  def validate_keyword_opts(_invalid), do: {:error, :invalid_schedule}

  @spec validate_duplicate_opts(keyword()) :: :ok | {:error, term()}
  def validate_duplicate_opts(opts) when is_list(opts) do
    duplicates =
      opts
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(fn {_key, count} -> count > 1 end)
      |> Enum.map(&elem(&1, 0))

    case duplicates do
      [] -> :ok
      values -> {:error, {:duplicate_schedule_opts, values}}
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
    supported = [:cron, :timezone, :missed, :overlap, :active]

    case Keyword.keys(opts) -- supported do
      [] -> :ok
      unknown -> {:error, {:unsupported_schedule_opts, unknown}}
    end
  end

  defp fetch_cron(opts) do
    case Keyword.fetch(opts, :cron) do
      {:ok, cron} when is_binary(cron) ->
        cron = String.trim(cron)

        if byte_size(cron) > 0 and valid_cron_expression?(cron) do
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
    timezone = String.trim(timezone)

    if timezone == "" do
      {:error, {:invalid_schedule_timezone, timezone}}
    else
      if Favn.Timezone.valid_identifier?(timezone) do
        {:ok, timezone}
      else
        {:error, {:invalid_schedule_timezone, timezone}}
      end
    end
  end

  defp validate_timezone(other), do: {:error, {:invalid_schedule_timezone, other}}

  defp valid_cron_expression?(value) when is_binary(value) do
    case String.split(value, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        cron_field_valid?(minute, 0, 59) and
          cron_field_valid?(hour, 0, 23) and
          cron_field_valid?(day, 1, 31) and
          cron_field_valid?(month, 1, 12) and
          cron_field_valid?(weekday, 0, 7)

      _other ->
        false
    end
  end

  defp cron_field_valid?(field, min, max) when is_binary(field) do
    tokens = String.split(field, ",", trim: false)

    Enum.all?(tokens, fn token ->
      token = String.trim(token)
      token != "" and cron_token_valid?(token, min, max)
    end)
  end

  defp cron_token_valid?("*", _min, _max), do: true

  defp cron_token_valid?(token, min, max) do
    case String.split(token, "/", parts: 2) do
      [base] ->
        cron_base_valid?(base, min, max)

      [base, step] ->
        cron_base_valid?(base, min, max) and positive_int?(step)

      _ ->
        false
    end
  end

  defp cron_base_valid?("*", _min, _max), do: true

  defp cron_base_valid?(base, min, max) do
    case String.split(base, "-", parts: 2) do
      [single] ->
        cron_number_in_range?(single, min, max)

      [from, to] ->
        case {parse_int(from), parse_int(to)} do
          {{:ok, left}, {:ok, right}} when left <= right and left >= min and right <= max ->
            true

          _ ->
            false
        end

      _ ->
        false
    end
  end

  defp cron_number_in_range?(value, min, max) do
    case parse_int(value) do
      {:ok, int} -> int >= min and int <= max
      :error -> false
    end
  end

  defp positive_int?(value) do
    case parse_int(value) do
      {:ok, int} -> int > 0
      :error -> false
    end
  end

  defp parse_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp validate_missed(value) when value in [:skip, :one, :all], do: {:ok, value}
  defp validate_missed(other), do: {:error, {:invalid_schedule_missed, other}}

  defp validate_active(value) when is_boolean(value), do: {:ok, value}
  defp validate_active(other), do: {:error, {:invalid_schedule_active, other}}
  defp validate_overlap(value) when value in [:forbid, :allow, :queue_one], do: {:ok, value}
  defp validate_overlap(other), do: {:error, {:invalid_schedule_overlap, other}}
end
