defmodule FavnLocal.ConfigTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Logger

  alias FavnLocal.Config
  alias FavnLocal.Locator
  alias FavnOrchestrator.Auth.ServiceTokens

  @pin_key Base.encode64(String.duplicate("k", 32))

  test "requires the caller to provide PostgreSQL and runtime secrets" do
    assert {:error, {:missing_env, "FAVN_DATABASE_URL"}} = Config.load(env: %{})

    assert {:error, {:missing_env, "FAVN_RUNTIME_INPUT_PIN_KEY"}} =
             Config.load(env: %{"FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn"})
  end

  test "loads a Docker-free development configuration" do
    assert {:ok, config} =
             Config.load(
               root_dir: ".",
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
               }
             )

    assert config.workspace_id == "local-dev"
    assert config.orchestrator_port == 4101
    assert config.view_port == 4173
    assert config.log_level == :info
    assert config.runtime_input_pin_key == String.duplicate("k", 32)
    assert config.runner_release_id =~ ~r/^rr_[0-9a-f]{64}$/
  end

  test "loads an explicit source-development log level and rejects invalid values" do
    assert {:ok, config} =
             Config.load(
               env:
                 valid_env(%{
                   "FAVN_LOG_LEVEL" => "debug"
                 })
             )

    assert config.log_level == :debug

    assert {:error, {:invalid_env, "FAVN_LOG_LEVEL", _expected}} =
             Config.load(env: valid_env(%{"FAVN_LOG_LEVEL" => "verbose"}))
  end

  test "rejects an invalid runtime input pin key" do
    assert {:error, {:invalid_secret_env, "FAVN_RUNTIME_INPUT_PIN_KEY", :invalid_key}} =
             Config.load(
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => "short"
               }
             )
  end

  test "reuses the local UI password across restarts" do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_local_config_#{System.unique_integer([:positive])}"
      )

    credentials = Path.join([root_dir, ".favn", "local", "credentials.json"])
    File.mkdir_p!(Path.dirname(credentials))
    File.write!(credentials, JSON.encode!(%{"view_password" => "stable-password"}))
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, config} =
             Config.load(
               root_dir: root_dir,
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
               }
             )

    assert config.bootstrap_password == "stable-password"
  end

  test "reuses the View cookie secret after local credentials are written" do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_local_session_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, first} = Config.load(root_dir: root_dir, env: valid_env())
    assert :ok = Locator.write(first, first.runner_release_id)
    assert {:ok, restarted} = Config.load(root_dir: root_dir, env: valid_env())

    assert restarted.bootstrap_password == first.bootstrap_password
    assert restarted.view_secret_key_base == first.view_secret_key_base
  end

  test "rotates the View cookie secret when the configured workspace changes" do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_local_workspace_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, first} =
             Config.load(root_dir: root_dir, workspace_id: "workspace-one", env: valid_env())

    assert :ok = Locator.write(first, first.runner_release_id)

    assert {:ok, changed} =
             Config.load(root_dir: root_dir, workspace_id: "workspace-two", env: valid_env())

    assert changed.bootstrap_password == first.bootstrap_password
    refute changed.view_secret_key_base == first.view_secret_key_base
  end

  test "applies complete View configuration without consumer endpoint config" do
    preserve_runtime_state()

    Application.delete_env(:favn_view, FavnView.Endpoint)
    Application.delete_env(:favn_view, :session_cookie_options)

    root_dir =
      Path.join(System.tmp_dir!(), "favn_local_config_#{System.unique_integer([:positive])}")

    assert {:ok, config} =
             Config.load(
               root_dir: root_dir,
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
               }
             )

    assert :ok = Config.apply(config)

    endpoint = Application.fetch_env!(:favn_view, FavnView.Endpoint)
    assert endpoint[:adapter] == Bandit.PhoenixAdapter
    assert endpoint[:pubsub_server] == FavnView.PubSub
    assert endpoint[:render_errors][:formats][:html] == FavnView.ErrorHTML
    assert endpoint[:render_errors][:formats][:json] == FavnView.ErrorJSON
    assert endpoint[:live_view][:signing_salt] == "Pqi8zx5Q"
    assert endpoint[:server]
    assert endpoint[:http] == [ip: {127, 0, 0, 1}, port: config.view_port]
    assert endpoint[:secret_key_base] == config.view_secret_key_base

    session = Application.fetch_env!(:favn_view, :session_cookie_options)
    assert session[:store] == :cookie
    assert session[:http_only]
    refute session[:secure]

    assert Application.fetch_env!(:favn_view, :source_development_passwordless_login) == %{
             workspace_id: config.workspace_id,
             username: "admin",
             capability: config.service_token
           }

    assert Application.fetch_env!(:favn_orchestrator, :trusted_local_development_auth) == %{
             workspace_id: config.workspace_id,
             username: "admin",
             capability_hash: ServiceTokens.hash_token(config.service_token)
           }

    assert [
             %{
               service_identity: "favn-local",
               enabled: true,
               platform_roles: [:platform_operator]
             }
           ] = ServiceTokens.configured_tokens()

    assert :ok = Config.clear_source_development_auth()
    assert Application.get_env(:favn_view, :source_development_passwordless_login) == nil
    assert Application.get_env(:favn_orchestrator, :trusted_local_development_auth) == nil
  end

  test "source development suppresses debug logs by default and honors explicit debug" do
    preserve_runtime_state()

    assert {:ok, info_config} = Config.load(env: valid_env())
    assert :ok = Config.apply(info_config)

    info_output =
      capture_log(fn ->
        Logger.debug("hidden-source-development-debug")
        Logger.info("visible-source-development-info")
      end)

    refute info_output =~ "hidden-source-development-debug"
    assert info_output =~ "visible-source-development-info"

    assert {:ok, debug_config} =
             Config.load(env: valid_env(%{"FAVN_LOG_LEVEL" => "debug"}))

    assert :ok = Config.apply(debug_config)

    debug_output =
      capture_log(fn ->
        Logger.debug("visible-source-development-debug")
      end)

    assert debug_output =~ "visible-source-development-debug"
  end

  defp preserve_runtime_state do
    applications = [:favn_storage_postgres, :favn_orchestrator, :favn_view]
    previous_env = Map.new(applications, &{&1, Application.get_all_env(&1)})
    previous_primary_level = Logger.level()
    previous_handler_level = handler_level()

    on_exit(fn ->
      restore_application_env(previous_env)
      :ok = Logger.configure(level: previous_primary_level)
      restore_handler_level(previous_handler_level)
    end)
  end

  defp valid_env(overrides \\ %{}) do
    Map.merge(
      %{
        "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
        "FAVN_RUNTIME_INPUT_PIN_KEY" => @pin_key
      },
      overrides
    )
  end

  defp restore_application_env(previous_env) do
    Enum.each(previous_env, fn {application, previous} ->
      application
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(application, &1))

      Enum.each(previous, fn {key, value} ->
        Application.put_env(application, key, value)
      end)
    end)
  end

  defp handler_level do
    case :logger.get_handler_config(:default) do
      {:ok, %{level: level}} -> {:present, level}
      {:error, {:not_found, :default}} -> :missing
    end
  end

  defp restore_handler_level({:present, level}),
    do: :logger.set_handler_config(:default, :level, level)

  defp restore_handler_level(:missing), do: :ok
end
