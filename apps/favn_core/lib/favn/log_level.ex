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

  @spec configure_from_env(map()) :: :ok | {:error, :invalid_log_level}
  def configure_from_env(environment) when is_map(environment) do
    case Map.fetch(environment, "FAVN_LOG_LEVEL") do
      :error ->
        :ok

      {:ok, value} ->
        with {:ok, level} <- parse(value),
             :ok <- Logger.configure(level: level),
             :ok <- :logger.set_handler_config(:default, :level, level) do
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
end
