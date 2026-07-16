defmodule Favn.UmbrellaTestRunner do
  @moduledoc false

  @apps [
    :favn_test_support,
    :favn_core,
    :favn_authoring,
    :favn_azure,
    :favn,
    :favn_sql_runtime,
    :favn_runner,
    :favn_orchestrator,
    :favn_storage_postgres,
    :favn_storage_sqlite,
    :favn_duckdb,
    :favn_duckdb_adbc,
    :favn_local,
    :favn_view
  ]

  @fast_tier_args [
    "--exclude",
    "acceptance",
    "--exclude",
    "slow",
    "--exclude",
    "browser"
  ]

  @type app :: atom()
  @type app_runner :: (app(), [String.t()] -> non_neg_integer())
  @type writer :: (String.t() -> term())

  @spec apps() :: [app()]
  def apps, do: @apps

  @spec child_test_args([String.t()]) :: [String.t()]
  def child_test_args(args) when is_list(args), do: @fast_tier_args ++ args

  @spec run([String.t()], app_runner(), writer()) :: non_neg_integer()
  def run(args, app_runner \\ &run_app/2, writer \\ &IO.puts/1)
      when is_list(args) and is_function(app_runner, 2) and is_function(writer, 1) do
    child_args = child_test_args(args)

    failures =
      Enum.reduce(@apps, [], fn app, failures ->
        writer.("\n==> #{app}")

        case app_runner.(app, child_args) do
          0 -> failures
          status when is_integer(status) and status > 0 -> [{app, status} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] ->
        writer.("\nAll umbrella fast-test slices passed.")
        0

      failures ->
        summary = Enum.map_join(failures, ", ", fn {app, status} -> "#{app}=#{status}" end)
        writer.("\nUmbrella fast-test failures: #{summary}")
        1
    end
  end

  defp run_app(app, args) do
    mix = System.find_executable("mix") || raise "mix executable not found"

    port =
      Port.open(
        {:spawn_executable, String.to_charlist(mix)},
        [
          :binary,
          :exit_status,
          :use_stdio,
          :stderr_to_stdout,
          {:args, ["do", "--app", Atom.to_string(app), "cmd", "mix", "test" | args]},
          {:env, child_env()}
        ]
      )

    await_exit(port)
  end

  defp await_exit(port) do
    receive do
      {^port, {:data, data}} ->
        IO.binwrite(data)
        await_exit(port)

      {^port, {:exit_status, status}} ->
        status
    end
  end

  defp child_env do
    [{~c"MIX_ENV", ~c"test"}] ++
      if(match?({:unix, _}, :os.type()) and File.dir?("/tmp"),
        do: [{~c"TMPDIR", ~c"/tmp"}],
        else: []
      )
  end
end
