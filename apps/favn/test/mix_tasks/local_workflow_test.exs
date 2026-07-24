defmodule Mix.Tasks.Favn.LocalWorkflowTest do
  use ExUnit.Case, async: false

  alias Favn.CLI.Init
  alias FavnLocal.Config

  @removed_tasks ~w(
    favn.build.control_plane.ex
    favn.dev.configured.ex
    favn.inspect.configured.ex
    favn.install.ex
    favn.logs.ex
    favn.maintainer.dev.configured.ex
    favn.maintainer.dev.ex
    favn.query.configured.ex
    favn.reload.configured.ex
    favn.reset.ex
    favn.status.ex
  )

  test "Docker-era public tasks and dotenv bootstrap stay removed" do
    task_dir = Path.expand("../../lib/mix/tasks", __DIR__)

    Enum.each(@removed_tasks, fn task ->
      refute File.exists?(Path.join(task_dir, task)), "obsolete task returned: #{task}"
    end)

    source =
      Path.expand("../../lib", __DIR__)
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.map_join("\n", &File.read!/1)

    refute source =~ "alias Favn.Dev"
    refute source =~ "Favn.Dev."
    refute source =~ "FAVN_ENV_FILE"
    refute source =~ "FAVN_CHECKOUT"
  end

  test "development configuration never requires Docker" do
    assert {:ok, _config} =
             Config.load(
               env: %{
                 "FAVN_DATABASE_URL" => "ecto://postgres:postgres@localhost/favn",
                 "FAVN_RUNTIME_INPUT_PIN_KEY" => Base.encode64(String.duplicate("k", 32))
               }
             )
  end

  test "generated DuckDB sample has usable project-relative paths and current dev config" do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_sample_init_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule SampleApp.MixProject do
        use Mix.Project
        def project, do: [app: :sample_app, version: "0.1.0", deps: []]
      end
      """
    )

    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, _result} =
             Init.run(
               root_dir: root_dir,
               duckdb: true,
               sample: true,
               app: :sample_app,
               base_module: "SampleApp"
             )

    config = File.read!(Path.join([root_dir, "config", "config.exs"]))
    env_example = File.read!(Path.join(root_dir, ".env.example"))

    assert config =~ ~s(database: ".data/local_smoke.duckdb")
    assert config =~ ~s(database_path: ".data/raw.duckdb")
    assert config =~ ~s(database_path: ".data/mart.duckdb")
    assert config =~ "dev: ["
    refute config =~ "FAVN_LOCAL_SAMPLE_"
    refute config =~ "local: ["

    assert env_example =~ "Favn does not load this file"
    assert env_example =~ "FAVN_DATABASE_URL="
    assert env_example =~ "FAVN_RUNTIME_INPUT_PIN_KEY="
  end

  test "deployment template is copied once and remains customer-owned" do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_deployment_init_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule Example.MixProject do\nend\n")
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, result} = Init.run(root_dir: root_dir, target: :deployment)
    assert result.target == :deployment
    assert File.regular?(Path.join(result.output, "compose.yml"))
    assert File.regular?(Path.join(result.output, "runner.Dockerfile"))
    assert File.regular?(Path.join(result.output, "env.example"))

    File.write!(Path.join(result.output, "compose.yml"), "customer-owned")
    output = result.output

    assert {:error, {:deployment_target_exists, ^output}} =
             Init.run(root_dir: root_dir, target: :deployment)

    assert File.read!(Path.join(result.output, "compose.yml")) == "customer-owned"
  end
end
