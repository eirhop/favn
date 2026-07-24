defmodule Favn.LogLevelTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Favn.LogLevel
  require Logger

  setup do
    primary_level = :logger.get_primary_config().level
    {:ok, handler} = :logger.get_handler_config(:default)

    on_exit(fn ->
      :ok = Logger.configure(level: primary_level)
      :ok = :logger.set_handler_config(:default, :level, handler.level)
    end)

    :ok
  end

  test "an absent variable leaves the configured info default unchanged" do
    :ok = Logger.configure(level: :info)
    :ok = :logger.set_handler_config(:default, :level, :info)

    assert :ok = LogLevel.configure_from_env(%{})
    assert :logger.get_primary_config().level == :info
    assert {:ok, %{level: :info}} = :logger.get_handler_config(:default)
  end

  test "debug configures both Logger filters and emits debug logs" do
    :ok = Logger.configure(level: :info)
    :ok = :logger.set_handler_config(:default, :level, :info)

    assert :ok = LogLevel.configure_from_env(%{"FAVN_LOG_LEVEL" => "debug"})
    assert :logger.get_primary_config().level == :debug
    assert {:ok, %{level: :debug}} = :logger.get_handler_config(:default)

    assert capture_log([level: :debug], fn ->
             Logger.debug("favn debug release probe")
           end) =~ "favn debug release probe"
  end

  test "accepts exactly the supported Logger levels" do
    for level <- ~w(debug info notice warning error critical alert emergency) do
      expected = String.to_existing_atom(level)
      assert {:ok, ^expected} = LogLevel.parse(level)
    end
  end

  test "invalid and injectable values fail without changing Logger" do
    :ok = Logger.configure(level: :warning)
    :ok = :logger.set_handler_config(:default, :level, :warning)

    for value <- ["verbose", "debug -s init stop", "debug\nerror"] do
      assert {:error, :invalid_log_level} =
               LogLevel.configure_from_env(%{"FAVN_LOG_LEVEL" => value})

      assert :logger.get_primary_config().level == :warning
      assert {:ok, %{level: :warning}} = :logger.get_handler_config(:default)
    end
  end
end
