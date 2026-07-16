# Runner Plugins And Runner-Local Services

A Favn runner is an isolated OTP application. Your consumer application's
supervision tree is not guaranteed to be running inside it. A runner plugin is
the public way to start services that asset code needs in that process.

In simple terms: a plugin tells Favn, “start these processes before you run my
assets, supervise them, and stop them with the runner.”

Good examples are credential caches, API session managers, client pools, and
rate limiters. The public contract is `Favn.Runner.Plugin`; consumers do not
depend on the internal `:favn_runner` app.

## The Simple Path

For ordinary OTP child specifications, use the built-in plugin:

```elixir
config :favn,
  runner_plugins: [
    {Favn.Runner.SupervisedChildren,
     children: [
       MyApp.RuntimeCache,
       {MyApp.ApiSession, endpoint: "https://example.internal"}
     ]}
  ]
```

The children start before the runner accepts asset work. Child startup failure
fails runner startup, and normal OTP restart rules apply.

## A Custom Plugin

Use a module when options need validation or the child list is computed:

```elixir
defmodule MyApp.RunnerPlugin do
  @behaviour Favn.Runner.Plugin

  @impl true
  def applications(_opts), do: {:ok, [:my_app_api]}

  @impl true
  def child_specs(opts) do
    with {:ok, endpoint} <- Keyword.fetch(opts, :endpoint) do
      {:ok, [{MyApp.ApiSession, endpoint: endpoint}]}
    else
      :error -> {:error, :missing_endpoint}
    end
  end
end

config :favn,
  runner_plugins: [
    {MyApp.RunnerPlugin, endpoint: "https://example.internal"}
  ]
```

`child_specs/1` runs once during runner startup and must return
`{:ok, children}` or `{:error, reason}`. Keep it fast and free of network calls;
the supervised child can do normal OTP initialization.

`applications/1` is optional. Use it when the plugin needs OTP applications
packaged with its dependency. Favn starts them before calling `child_specs/1`,
which is important for isolated local and deployed runners. Do not declare
`:favn_runner` or use this to start the consumer's entire application.

Favn bounds the number of plugins and children, validates child specs and
duplicate ids before startup, and reports callback failures without echoing
plugin options. Plugin callbacks also have a finite startup timeout.

## State Is An Escape Hatch, Not Storage

Plugin processes can keep state across many asset runs in the same runner. This
is useful, but the state is temporary:

- it exists only in one runner;
- another runner cannot see it;
- it disappears on restart, replacement, deployment, or rescheduling;
- Favn does not persist, replicate, replay, or order it between asset runs.

Use plugin state for data that can be rebuilt: tokens, pools, sessions,
rate-limit counters, or cached metadata. Do not use it for business data,
checkpoints, idempotency records, or cross-run messages that must survive.

A practical rule: if deleting the state could change the correct result, put it
in durable storage.

## Azure Credentials

Add the optional `:favn_azure` package and its runner plugin to get a shared,
runner-local token cache:

```elixir
config :favn,
  runner_plugins: [
    Favn.Azure.RunnerPlugin,
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

Asset code or another runner plugin calls the public API, not the cache process:

```elixir
{:ok, access_token} =
  Favn.Azure.Credentials.fetch_access_token(
    "https://vault.azure.net",
    provider: "managed_identity"
  )

MyApp.KeyVault.get_secret("orders-api", access_token)
```

The same API can be called from a GenServer that exchanges the Azure token for a
downstream session token and caches that session in its own state. Keep the
valid-session path fast and perform authentication outside the GenServer:

```elixir
@login_timeout_ms 10_000

# State starts as %{session: nil, login_task: nil, login_timer: nil}.
def handle_call(:session, _from, %{session: session} = state)
    when not is_nil(session) do
  if MyApp.Downstream.valid?(session),
    do: {:reply, {:ok, session}, state},
    else: start_login(state)
end

def handle_call(:session, _from, state), do: start_login(state)

defp start_login(%{login_task: nil} = state) do
  task =
    Task.Supervisor.async_nolink(MyApp.RunnerTasks, fn ->
      with {:ok, azure_token} <-
             Favn.Azure.Credentials.fetch_access_token(
               "api://downstream-service",
               provider: "managed_identity"
             ) do
        MyApp.Downstream.login(azure_token)
      end
    end)

  timer =
    Process.send_after(self(), {:login_timeout, task.ref}, @login_timeout_ms)

  {:reply, {:error, :session_refreshing},
   %{state | login_task: task, login_timer: timer}}
end

defp start_login(state),
  do: {:reply, {:error, :session_refreshing}, state}

def handle_info({ref, result}, %{login_task: %{ref: ref}} = state) do
  Process.demonitor(ref, [:flush])
  state = clear_login(state)

  case result do
    {:ok, session} -> {:noreply, %{state | session: session}}
    {:error, _reason} -> {:noreply, state}
    _invalid_result -> {:noreply, state}
  end
end

def handle_info({:login_timeout, ref},
      %{login_task: %Task{ref: ref} = task} = state) do
  Task.shutdown(task, :brutal_kill)
  {:noreply, clear_login(state)}
end

def handle_info({:DOWN, ref, :process, _pid, _reason},
      %{login_task: %{ref: ref}} = state) do
  {:noreply, clear_login(state)}
end

defp clear_login(state) do
  if state.login_timer, do: Process.cancel_timer(state.login_timer)
  %{state | login_task: nil, login_timer: nil}
end
```

`MyApp.Downstream.valid?/1` should only inspect local expiry/session metadata; it
must not make a network call from `handle_call/3`. The timer above bounds and
cancels both Azure acquisition and downstream login work.

For callers that should wait instead of retrying, store their `from` values and
reply with `GenServer.reply/2` when the task completes. Bound that waiter list,
handle task failure/timeout, and supervise `MyApp.RunnerTasks` through the same
runner plugin.

The Azure cache performs one provider fetch for concurrent callers with the same
resource, identity, and provider options. It refreshes on demand before expiry.
If refresh fails, it may return the old token only while that token is still
valid; it never returns an expired token. Provider work runs outside the cache
GenServer; request sizes, global in-flight work, per-key waiters, entry count,
and call duration are bounded. Direct provider work also runs in an owned task
with a finite timeout. Tokens and provider error details are redacted from
Inspect output and Favn diagnostics.

Use `provider: "cli"` for a developer signed in through `az login`. Use
`provider: "managed_identity"` for Azure App Service or IMDS; `client_id`
selects a user-assigned identity when needed. Those are the only built-in
provider names. Atom forms and the legacy `azure_cli` name are rejected with a
structured configuration error.

Because both names match DuckDB's native Azure credential-chain values, one
environment value can configure both systems without conversion:

```elixir
provider = System.fetch_env!("AZURE_CREDENTIAL_PROVIDER")

token_ref =
  Favn.Azure.Credentials.token_ref(
    "https://storage.azure.com/",
    provider: provider
  )

duckdb_chain = provider
```

Pass `duckdb_chain` to the deployment's trusted native DuckDB `CHAIN`
parameter. Custom Elixir providers remain separate: pass a module implementing
`Favn.Azure.CredentialProvider` instead of a built-in string.

Calls in an application without the plugin fetch directly with a finite timeout;
`cache: false` makes that choice explicit. When the plugin is configured, a
temporarily unavailable default cache returns a retryable error instead of
bypassing cache concurrency and single-flight bounds. In a runner, configure
`Favn.Azure.RunnerPlugin` so repeated calls are reused.

## DuckDB Injection

DuckDB session-script parameters accept a deferred Azure token ref:

```elixir
config :favn,
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [
        resources: [
          landing_storage: [
            file: {:priv, :my_app, "duckdb/landing_storage.sql"},
            params: [
              azure_token:
                Favn.Azure.Credentials.token_ref(
                  "https://storage.azure.com/",
                  provider: "managed_identity"
                )
            ]
          ]
        ]
      ]
    ]
  ]
```

The ref contains the request, not the token. Favn resolves it once while
preparing the pool identity for a new physical session, reuses that exact plan
during bootstrap, renders the value as a secret `@azure_token` parameter, and
includes only its hash in pool identity. A refreshed token therefore creates a
new session identity instead of accidentally reusing a session initialized with
an expired token. The trusted SQL file still owns the exact native DuckDB
`CREATE SECRET` syntax.

### DuckLake PostgreSQL With Managed Identity

Azure Database for PostgreSQL uses this token audience:

```elixir
access_token =
  Favn.Azure.Credentials.token_ref(
    "https://ossrdbms-aad.database.windows.net",
    provider: "managed_identity"
  )
```

Pass that ref as a secret resource parameter, along with the non-secret
PostgreSQL connection values:

```elixir
source_metadata: [
  file: {:priv, :my_app, "duckdb/source_metadata.sql"},
  params: [
    host: "my-server.postgres.database.azure.com",
    port: 5432,
    database: "ducklake",
    user: "favn-runner",
    access_token: access_token,
    data_path: "az://lake/source"
  ]
]
```

The trusted `source_metadata.sql` uses the access token as DuckDB's PostgreSQL
secret password:

```sql
CREATE OR REPLACE SECRET source_metadata (
  TYPE postgres,
  HOST @host,
  PORT @port,
  DATABASE @database,
  USER @user,
  PASSWORD @access_token
);
ATTACH 'ducklake:postgres:sslmode=require' AS source (
  DATA_PATH @data_path,
  META_SECRET source_metadata
);
```

This works with both in-process DuckDB and DuckDB ADBC. Favn asks the Azure
cache for the current token while preparing each checkout. A compatible pooled
session is reused without rerunning the SQL. After refresh changes the token
fingerprint, the pool closes the superseded idle physical session, releases its
admission lease, and bootstraps a new physical session with the refreshed
password. Active sessions finish their current work and close on checkin.

In simple terms: the token is fetched just in time to choose a DuckDB session.
If the token is unchanged, Favn reuses the session. If it changed, Favn replaces
the session before using PostgreSQL again.

This injection boundary is provider-neutral: another package can create a
secret `Favn.RuntimeValue` backed by an AWS credential service without changing
DuckDB or the runner lifecycle. Runtime-value provider callbacks run in an owned
process with a finite 15-second bound.

## What This Does Not Do

- It does not start the consumer application's whole supervision tree.
- It does not transport arbitrary plugin configuration; supported local runner
  config transport remains intentionally bounded.
- It does not make plugin state durable or distributed.
- It does not add Azure authentication to Favn control-plane storage.
- It does not make arbitrary runtime-value refs resolve everywhere. Each
  integration point must explicitly support them; DuckDB session-script
  parameters are the first supported boundary.

## Related Docs

- `Favn.Runner.Plugin`
- `Favn.Runner.SupervisedChildren`
- `Favn.Azure.RunnerPlugin`
- `Favn.Azure.Credentials`
- `Favn.RuntimeValue`
- [DuckDB Session Scripts And Resources](duckdb-session-scripts.md)
- [Configuration](configuration.md)
- [Runtime Model](runtime-model.md)
