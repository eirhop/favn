defmodule Favn.Runner.Plugin do
  @moduledoc """
  Public lifecycle extension for services that must live inside a Favn runner.

  A runner is an isolated OTP application. The consumer application's own
  supervision tree may not be running there, so long-lived runtime services
  such as credential caches, client pools, rate limiters, and session managers
  belong under this lifecycle when assets need them.

  ## Example

      defmodule MyApp.RunnerPlugin do
        @behaviour Favn.Runner.Plugin

        @impl true
        def child_specs(opts) do
          {:ok, [{MyApp.SessionCache, opts}]}
        end
      end

      config :favn,
        runner_plugins: [
          {MyApp.RunnerPlugin, max_entries: 100}
        ]

  The callback runs once during runner startup. Its children start before the
  runner accepts asset work and follow ordinary OTP restart semantics. Return a
  tagged error for invalid configuration; raising is reserved for bugs.

  A plugin whose callback or children require another OTP application may also
  implement `applications/1`. Favn starts those applications before invoking
  `child_specs/1`. Declare only applications packaged with the consumer plugin;
  do not declare `:favn_runner` itself.

  ## State lifetime

  Plugin state is local to one runner process and may disappear at any time
  when the runner is restarted, replaced, or rescheduled. It is an appropriate
  home for rebuildable caches, pools, sessions, and rate-limit state. It is not
  durable storage and must not be used for business data, checkpoints,
  idempotency records, or communication that must survive a runner restart.

  A useful rule is: if deleting the state could change the correct result, keep
  it in durable storage instead.
  """

  @type child_spec :: Supervisor.child_spec() | {module(), term()} | module()
  @type plugin_opts :: keyword()
  @type plugin_entry :: module() | {module(), plugin_opts()}

  @callback applications(plugin_opts()) ::
              {:ok, [atom()]} | {:error, term()}

  @callback child_specs(plugin_opts()) ::
              {:ok, [child_spec()]} | {:error, term()}

  @optional_callbacks applications: 1
end
