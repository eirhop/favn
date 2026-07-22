defmodule Favn.Dev.OutputRedactorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Favn.Dev.{ComposeEnv, Docker, Paths, State}

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_output_redactor_#{System.unique_integer([:positive])}")

    secret = "generated-service-secret-value"
    runner_secret = "customer-runtime-secret-value"
    multiline_secret = "first-sensitive-line\nsecond-sensitive-line"
    :ok = State.write_secrets(%{"service_token" => secret}, root_dir: root_dir)

    {:ok, runner_env} =
      ComposeEnv.encode(%{
        "CUSTOM_TOKEN" => runner_secret,
        "MULTILINE_TOKEN" => multiline_secret
      })

    runner_env_path = Paths.compose_runner_env_path(root_dir)
    File.mkdir_p!(Path.dirname(runner_env_path))
    File.write!(runner_env_path, runner_env)

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{
      root_dir: root_dir,
      secret: secret,
      runner_secret: runner_secret,
      multiline_secret: multiline_secret
    }
  end

  test "Docker output is redacted before return and across streamed chunk boundaries", context do
    parent = self()

    runner = fn "docker", _args, opts ->
      writer = Keyword.fetch!(opts, :output_writer)
      writer.("stream #{String.slice(context.secret, 0, 12)}")
      writer.(String.slice(context.secret, 12..-1//1) <> "\n")
      writer.("runner=#{context.runner_secret}\n")
      [first_line, second_line] = String.split(context.multiline_secret, "\n")
      writer.("multiline=#{first_line}\n")
      writer.(second_line <> "\n")
      writer.("database=ecto://user:password@postgres.internal/db\n")

      {"return #{context.secret} #{context.runner_secret} " <>
         "ecto://user:password@postgres.internal/db", 1}
    end

    project = %{
      "project_name" => "favn-redaction-test",
      "compose_path" => Path.join(context.root_dir, "compose.yml"),
      "env_path" => Path.join(context.root_dir, ".env")
    }

    streamed =
      capture_io(fn ->
        result =
          Docker.compose(project, ["logs", "--follow"],
            root_dir: context.root_dir,
            docker_executable: "docker",
            docker_command_runner: runner,
            docker_output_writer: &IO.binwrite/1
          )

        send(parent, {:result, result})
      end)

    assert_receive {:result, {returned, 1}}

    for output <- [streamed, returned] do
      assert output =~ "[REDACTED]"
      refute output =~ context.secret
      refute output =~ context.runner_secret
      refute output =~ "first-sensitive-line"
      refute output =~ "second-sensitive-line"
      refute output =~ "ecto://user:password@postgres.internal/db"
    end
  end
end
