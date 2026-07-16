defmodule Favn.Azure.RunnerPlugin do
  @moduledoc """
  Favn runner plugin that owns the runner-local Azure credential cache.

  Configure it alongside execution plugins:

      config :favn,
        runner_plugins: [
          Favn.Azure.RunnerPlugin,
          FavnDuckdb
        ]

  Supported options are `:refresh_before_seconds`, `:fetch_timeout`,
  `:max_entries`, `:max_inflight`, and `:max_waiters_per_key`. Defaults are
  bounded and suitable for ordinary runner deployments.
  """

  @behaviour Favn.Runner.Plugin

  alias Favn.Azure.Credentials.Supervisor, as: CredentialsSupervisor

  @allowed_options [
    :refresh_before_seconds,
    :fetch_timeout,
    :max_entries,
    :max_inflight,
    :max_waiters_per_key
  ]

  @impl true
  def applications(_opts), do: {:ok, [:favn_azure]}

  @impl true
  def child_specs(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- @allowed_options == [] do
      {:ok, [{CredentialsSupervisor, opts}]}
    else
      {:error, :invalid_azure_runner_plugin_options}
    end
  end

  def child_specs(_opts), do: {:error, :invalid_azure_runner_plugin_options}
end
