defmodule FavnControlPlane.Release do
  @moduledoc false

  @applications [:favn_core, :favn_storage_postgres, :favn_orchestrator, :favn_view]

  @doc false
  @spec applications() :: [atom()]
  def applications, do: @applications

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
