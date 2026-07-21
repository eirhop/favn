defmodule FavnControlPlane.Release do
  @moduledoc false

  @applications [:favn_core, :favn_storage_postgres, :favn_orchestrator, :favn_view]

  # These are the production-active, direct external build dependencies of the
  # four release applications. The input collector follows their non-optional
  # lock dependencies transitively and fails if any root cannot be resolved.
  @dependency_roots [
    :argon2_elixir,
    :bandit,
    :decimal,
    :ecto_sql,
    :esbuild,
    :gettext,
    :heroicons,
    :jason,
    :phoenix,
    :phoenix_html,
    :phoenix_live_dashboard,
    :phoenix_live_view,
    :phoenix_pubsub,
    :phoenix_storybook,
    :postgrex,
    :tailwind,
    :telemetry_metrics,
    :telemetry_poller,
    :tz
  ]

  @doc false
  @spec applications() :: [atom()]
  def applications, do: @applications

  @doc false
  @spec dependency_roots() :: [atom()]
  def dependency_roots, do: @dependency_roots

  @doc false
  @spec config() :: keyword()
  def config do
    [
      favn_control_plane: [
        version: {:from_app, :favn_orchestrator},
        applications: Enum.map(@applications, &{&1, :permanent}),
        include_executables_for: [:unix],
        rel_templates_path: "rel/control_plane",
        strip_beams: true
      ]
    ]
  end
end
