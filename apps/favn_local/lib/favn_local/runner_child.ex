defmodule FavnLocal.RunnerChild do
  @moduledoc false

  alias FavnLocal.Config

  @type child :: %{port: port(), node: node(), release_id: String.t()}

  @spec start(Config.t(), String.t()) :: {:ok, child()} | {:error, term()}
  def start(%Config{} = config, release_id) when is_binary(release_id) do
    with executable when is_binary(executable) <- System.find_executable("elixir") do
      args =
        [
          "--name",
          Atom.to_string(config.runner_node),
          "--cookie",
          config.distribution_cookie
        ] ++
          code_path_args() ++
          [
            "-e",
            "FavnLocal.RunnerMain.run(#{inspect(config.root_dir)})"
          ]

      port =
        Port.open(
          {:spawn_executable, executable},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: args,
            cd: config.root_dir,
            env: [
              {~c"MIX_ENV", ~c"dev"},
              {~c"FAVN_LOCAL_OPERATOR_NODE",
               config.operator_node |> Atom.to_string() |> String.to_charlist()},
              {~c"FAVN_RUNNER_RELEASE_ID", String.to_charlist(release_id)}
            ]
          ]
        )

      {:ok, %{port: port, node: config.runner_node, release_id: release_id}}
    else
      _missing -> {:error, {:missing_tool, "elixir"}}
    end
  rescue
    error -> {:error, {:runner_start_failed, Exception.message(error)}}
  end

  @spec stop(child()) :: :ok
  def stop(%{node: runner_node}) do
    if Node.ping(runner_node) == :pong do
      :erpc.cast(runner_node, FavnLocal.RunnerMain, :stop, [])
    end

    :ok
  end

  @spec ready?(child()) :: boolean()
  def ready?(%{node: runner_node, release_id: release_id}) do
    with :pong <- Node.ping(runner_node),
         {:ok, %{runner_release_id: ^release_id, ready?: true}} <-
           :erpc.call(runner_node, FavnRunner, :diagnostics, [[]], 2_000) do
      true
    else
      _not_ready -> false
    end
  catch
    _kind, _reason -> false
  end

  defp code_path_args do
    :code.get_path()
    |> Enum.map(&List.to_string/1)
    |> Enum.filter(&File.dir?/1)
    |> Enum.flat_map(&["-pa", &1])
  end
end
