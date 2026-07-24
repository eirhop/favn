defmodule FavnLocal.RunnerMain do
  @moduledoc false

  @spec run(Path.t()) :: no_return()
  def run(root_dir) when is_binary(root_dir) do
    :ok = load_project_config(root_dir)

    case Application.ensure_all_started(:favn_runner) do
      {:ok, _applications} ->
        monitor_operator()
        IO.puts("Favn local runner ready: #{node()}")
        Process.sleep(:infinity)

      {:error, reason} ->
        raise "failed to start Favn runner: #{inspect(reason)}"
    end
  end

  @spec stop() :: :ok
  def stop do
    spawn(fn ->
      _ = Application.stop(:favn_runner)
      System.stop(0)
    end)

    :ok
  end

  defp monitor_operator do
    operator_node =
      "FAVN_LOCAL_OPERATOR_NODE"
      |> System.fetch_env!()
      |> String.to_atom()

    spawn(fn ->
      case Node.connect(operator_node) do
        true ->
          Node.monitor(operator_node, true)

          receive do
            {:nodedown, ^operator_node} -> System.stop(0)
          end

        false ->
          System.stop(1)
      end
    end)
  end

  defp load_project_config(root_dir) do
    config_dir = Path.join(root_dir, "config")

    [Path.join(config_dir, "config.exs"), Path.join(config_dir, "runtime.exs")]
    |> Enum.filter(&File.regular?/1)
    |> Enum.reduce([], fn path, acc ->
      Config.Reader.merge(acc, Config.Reader.read!(path, env: :dev, target: :host))
    end)
    |> Application.put_all_env(persistent: true)
  end
end
