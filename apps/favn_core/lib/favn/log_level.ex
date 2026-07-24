defmodule Favn.LogLevel do
  @moduledoc false

  @levels %{
    "debug" => :debug,
    "info" => :info,
    "notice" => :notice,
    "warning" => :warning,
    "error" => :error,
    "critical" => :critical,
    "alert" => :alert,
    "emergency" => :emergency
  }
  @supported_levels Map.values(@levels)

  @spec configure_from_env(map()) :: :ok | {:error, :invalid_log_level}
  def configure_from_env(environment) when is_map(environment) do
    case Map.fetch(environment, "FAVN_LOG_LEVEL") do
      :error ->
        :ok

      {:ok, value} ->
        with {:ok, level} <- parse(value),
             :ok <- configure(level) do
          :ok
        else
          _error -> {:error, :invalid_log_level}
        end
    end
  end

  @spec parse(term()) :: {:ok, Logger.level()} | {:error, :invalid_log_level}
  def parse(value) when is_binary(value) do
    case @levels do
      %{^value => level} -> {:ok, level}
      _levels -> {:error, :invalid_log_level}
    end
  end

  def parse(_value), do: {:error, :invalid_log_level}

  @spec configure(Logger.level()) :: :ok | {:error, :invalid_log_level}
  def configure(level) when level in @supported_levels do
    with :ok <- Logger.configure(level: level),
         :ok <- configure_default_handler(level) do
      :ok
    else
      _error -> {:error, :invalid_log_level}
    end
  end

  def configure(_level), do: {:error, :invalid_log_level}

  defp configure_default_handler(level) do
    case :logger.set_handler_config(:default, :level, level) do
      :ok -> :ok
      {:error, {:not_found, :default}} -> :ok
      error -> error
    end
  end
end
