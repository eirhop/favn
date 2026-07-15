defmodule Mix.Tasks.Favn.EnvBootstrapIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  @repo_root Path.expand("../../../..", __DIR__)
  @mode_env "FAVN_ENV_BOOTSTRAP_INTEGRATION_MODE"
  @capture_env "FAVN_ENV_BOOTSTRAP_INTEGRATION_CAPTURE"

  setup do
    consumer_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_env_bootstrap_consumer_#{System.unique_integer([:positive])}"
      )

    capture_path = Path.join(consumer_dir, "runtime_config.capture")
    previous_mode = System.get_env(@mode_env)

    System.delete_env(@mode_env)
    File.mkdir_p!(Path.join(consumer_dir, "config"))
    write_consumer_project!(consumer_dir)

    on_exit(fn ->
      File.rm_rf(consumer_dir)

      if previous_mode do
        System.put_env(@mode_env, previous_mode)
      else
        System.delete_env(@mode_env)
      end
    end)

    %{capture_path: capture_path, consumer_dir: consumer_dir}
  end

  @tag timeout: 120_000
  test "public dev and reload evaluate runtime config once with current .env", context do
    %{capture_path: capture_path, consumer_dir: consumer_dir} = context
    env_path = Path.join(consumer_dir, ".env")

    File.write!(env_path, "#{@mode_env}=cloud\n")

    {dev_output, 1} =
      run_mix(consumer_dir, ["favn.dev", "--root-dir", consumer_dir], capture_path)

    assert dev_output =~ "install required; run mix favn.install"

    assert %{database: "cloud.duckdb", evaluation_count: 1, mode: "cloud"} =
             read_capture!(capture_path)

    File.write!(env_path, "#{@mode_env}=local\n")

    {reload_output, 1} =
      run_mix(consumer_dir, ["favn.reload", "--root-dir", consumer_dir], capture_path)

    assert reload_output =~ "stack not running; use mix favn.dev"

    assert %{database: "local.duckdb", evaluation_count: 2, mode: "local"} =
             read_capture!(capture_path)
  end

  defp write_consumer_project!(consumer_dir) do
    File.write!(
      Path.join(consumer_dir, "mix.exs"),
      """
      defmodule FavnEnvBootstrapIntegrationConsumer.MixProject do
        use Mix.Project

        @repo_root #{inspect(@repo_root)}

        def project do
          [
            app: :favn_env_bootstrap_integration_consumer,
            version: "0.1.0",
            elixir: "~> 1.20",
            build_path: Path.join(@repo_root, "_build"),
            deps_path: Path.join(@repo_root, "deps"),
            lockfile: Path.join(@repo_root, "mix.lock"),
            deps: [{:favn, path: Path.join(@repo_root, "apps/favn")}]
          ]
        end

        def application, do: []
      end
      """
    )

    File.write!(
      Path.join(consumer_dir, "config/runtime.exs"),
      """
      import Config

      capture_path = System.fetch_env!(#{inspect(@capture_env)})
      mode = System.get_env(#{inspect(@mode_env)}) || "local"
      database = if mode == "cloud", do: "cloud.duckdb", else: "local.duckdb"

      evaluation_count =
        case File.read(capture_path) do
          {:ok, binary} -> :erlang.binary_to_term(binary, [:safe]).evaluation_count + 1
          {:error, :enoent} -> 1
        end

      File.write!(
        capture_path,
        :erlang.term_to_binary(%{
          database: database,
          evaluation_count: evaluation_count,
          mode: mode
        })
      )

      config :favn, :connections,
        ducklake: [
          open: [database: database],
          duckdb: [extensions: ["ducklake"]]
        ]
      """
    )
  end

  defp run_mix(consumer_dir, args, capture_path) do
    mix = System.find_executable("mix") || "mix"

    System.cmd(mix, args,
      cd: consumer_dir,
      env: %{"MIX_ENV" => "test", @capture_env => capture_path},
      stderr_to_stdout: true
    )
  end

  defp read_capture!(capture_path) do
    capture_path |> File.read!() |> :erlang.binary_to_term([:safe])
  end
end
