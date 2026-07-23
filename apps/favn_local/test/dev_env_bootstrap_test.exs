defmodule Favn.Dev.EnvBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.EnvBootstrap
  alias Favn.Dev.EnvFile

  @token_env "FAVN_INTERNAL_ENV_BOOTSTRAP"
  @loaded_key "FAVN_ENV_BOOTSTRAP_TEST_LOADED"
  @shell_key "FAVN_ENV_BOOTSTRAP_TEST_SHELL"
  @sample_keys [
    "FAVN_LOCAL_SAMPLE_DATABASE_PATH",
    "FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH",
    "FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH"
  ]

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_env_bootstrap_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)

    previous_env =
      Map.new(
        [@token_env, @loaded_key, @shell_key, "FAVN_ENV_FILE", "MIX_ENV"] ++ @sample_keys,
        fn key ->
          {key, System.get_env(key)}
        end
      )

    System.delete_env(@token_env)
    System.delete_env(@loaded_key)
    System.delete_env("FAVN_ENV_FILE")
    Enum.each(@sample_keys, &System.delete_env/1)
    System.put_env(@shell_key, "shell-value")

    on_exit(fn ->
      File.rm_rf(root_dir)

      Enum.each(previous_env, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end)

    %{root_dir: root_dir}
  end

  test "exec loads .env once and runs the guarded configured task", %{root_dir: root_dir} do
    env_path = Path.join(root_dir, ".env")
    database_path = Path.join(root_dir, "shell/local.duckdb")
    raw_catalog_path = Path.join(root_dir, "configured/raw.duckdb")
    System.put_env("FAVN_LOCAL_SAMPLE_DATABASE_PATH", database_path)

    File.write!(
      env_path,
      """
      #{@loaded_key}=file-value
      #{@shell_key}=file-value
      FAVN_ENV_FILE=ignored.env
      FAVN_INTERNAL_ENV_BOOTSTRAP=forged
      MIX_ENV=prod
      FAVN_LOCAL_SAMPLE_DATABASE_PATH=ignored-file-value
      FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH=#{raw_catalog_path}
      """
    )

    caller = self()

    command_runner = fn mix, args, command_opts ->
      assert String.starts_with?(Path.basename(mix), "mix")
      assert args == ["favn.dev.configured", "--root-dir", root_dir]
      assert command_opts[:stderr_to_stdout]
      assert command_opts[:into] != nil

      command_env = Map.new(command_opts[:env])
      token_key = @token_env
      token = Map.fetch!(command_env, token_key)
      assert token_key == @token_env
      assert command_env["MIX_ENV"] == Atom.to_string(Mix.env())

      assert {:ok, binary} = Base.url_decode64(token, padding: false)
      payload = :erlang.binary_to_term(binary, [:safe])
      assert payload["loaded_keys"] == Enum.sort([@loaded_key, @shell_key] ++ @sample_keys)
      refute Map.has_key?(payload, "values")

      File.write!(env_path, "#{@loaded_key}=changed-after-bootstrap\n")
      Enum.each(command_env, fn {key, value} -> System.put_env(key, value) end)

      assert {:ok, configured_opts} =
               EnvBootstrap.consume(:dev, root_dir: root_dir)

      assert {:ok, configured_opts} = EnvBootstrap.ensure_loaded(configured_opts)

      send(caller, {
        :configured,
        EnvFile.loaded_env(configured_opts),
        System.get_env(@shell_key),
        System.get_env(token_key)
      })

      {"", 0}
    end

    assert {:ok, 0} =
             EnvBootstrap.exec(
               :dev,
               ["--root-dir", root_dir],
               root_dir: root_dir,
               env_bootstrap_command_runner: command_runner
             )

    assert_received {:configured, loaded, "shell-value", nil}
    assert loaded[@loaded_key] == "file-value"
    assert loaded[@shell_key] == "shell-value"

    assert loaded["FAVN_LOCAL_SAMPLE_DATABASE_PATH"] == database_path

    assert loaded["FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH"] == raw_catalog_path

    assert loaded["FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH"] ==
             Path.join(root_dir, ".data/mart.duckdb")

    assert System.get_env(@loaded_key) == "file-value"
  end

  test "install preserves env-file errors", %{root_dir: root_dir} do
    System.put_env("FAVN_ENV_FILE", "missing.env")
    path = Path.join(root_dir, "missing.env")

    assert {:error, {:env_file_not_found, ^path}} =
             EnvBootstrap.install_for_current_process(:dev, root_dir: root_dir)

    assert System.get_env(@token_env) == nil
  end

  test "query uses its guarded configured task", %{root_dir: root_dir} do
    command_runner = fn _mix, args, command_opts ->
      assert args == ["favn.query.configured", "select 1", "--root-dir", root_dir]

      command_env = Map.new(command_opts[:env])
      Enum.each(command_env, fn {key, value} -> System.put_env(key, value) end)

      assert {:ok, _opts} = EnvBootstrap.consume(:query, root_dir: root_dir)
      {"", 0}
    end

    assert {:ok, 0} =
             EnvBootstrap.exec(
               :query,
               ["select 1", "--root-dir", root_dir],
               root_dir: root_dir,
               env_bootstrap_command_runner: command_runner
             )
  end

  test "maintainer dev uses its guarded configured task", %{root_dir: root_dir} do
    command_runner = fn _mix, args, command_opts ->
      assert args == ["favn.maintainer.dev.configured", "--root-dir", root_dir]

      command_env = Map.new(command_opts[:env])
      Enum.each(command_env, fn {key, value} -> System.put_env(key, value) end)

      assert {:ok, _opts} = EnvBootstrap.consume(:maintainer_dev, root_dir: root_dir)
      {"", 0}
    end

    assert {:ok, 0} =
             EnvBootstrap.exec(
               :maintainer_dev,
               ["--root-dir", root_dir],
               root_dir: root_dir,
               env_bootstrap_command_runner: command_runner
             )
  end

  test "consume rejects a different configured task and consumes the token", %{root_dir: root_dir} do
    assert :ok = EnvBootstrap.install_for_current_process(:dev, root_dir: root_dir)

    assert {:error, {:invalid_env_bootstrap, :context_mismatch}} =
             EnvBootstrap.consume(:reload, root_dir: root_dir)

    assert {:error, :env_bootstrap_required} =
             EnvBootstrap.consume(:dev, root_dir: root_dir)
  end

  test "consume rejects compressed external terms before decoding", %{root_dir: root_dir} do
    token =
      %{payload: String.duplicate("x", 4_096)}
      |> :erlang.term_to_binary(compressed: 9)
      |> Base.url_encode64(padding: false)

    System.put_env(@token_env, token)

    assert {:error, {:invalid_env_bootstrap, :compressed_payload}} =
             EnvBootstrap.consume(:dev, root_dir: root_dir)

    assert System.get_env(@token_env) == nil
  end
end
