defmodule FavnLocal.RunnerProcess do
  @moduledoc false

  @spec run(Path.t()) :: no_return()
  def run(root_dir) when is_binary(root_dir) do
    {:ok, _applications} = Application.ensure_all_started(:mix)
    :ok = load_project_config(root_dir)
    :ok = Favn.LogLevel.configure_from_env(System.get_env())

    case Application.ensure_all_started(:favn_runner) do
      {:ok, _applications} ->
        monitor_operator()
        announce_ready()
        Process.sleep(:infinity)

      {:error, reason} ->
        raise "failed to start Favn runner: #{inspect(reason)}"
    end
  end

  # This runner's stdout is a port owned by the stack that launched it, and that
  # stack can close the port while this process is still booting — a stop, or a
  # restart that abandoned it. Writing to the closed device then raises
  # `ErlangError :terminated`, and an `elixir -e` script has no `:standard_error`
  # left to report that on, so reporting it raised `badarg` and killed the VM
  # "during boot", leaving an `erl_crash.dump` in the project root and no trace of
  # the real reason. Failing to announce is not a failure worth dying for: the
  # operator-node monitor above already decides when this runner should stop.
  defp announce_ready do
    IO.puts("Favn local runner ready: #{node()}")
  rescue
    _no_device -> :ok
  catch
    _kind, _reason -> :ok
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
