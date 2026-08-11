defmodule FavnControlPlane.Release do
  @moduledoc false

  @orchestrator_applications [
    :favn_core,
    :favn_azure,
    :favn_storage_postgres,
    :favn_orchestrator
  ]

  @view_applications [
    {:favn_orchestrator, :load},
    {:favn_view, :permanent}
  ]

  @doc false
  @spec orchestrator_applications() :: [atom()]
  def orchestrator_applications, do: @orchestrator_applications

  @doc false
  @spec view_applications() :: [{atom(), :load | :permanent}]
  def view_applications, do: @view_applications

  @doc false
  @spec config() :: keyword()
  def config do
    [
      favn_orchestrator: [
        version: {:from_app, :favn_orchestrator},
        applications: Enum.map(@orchestrator_applications, &{&1, :permanent}),
        include_executables_for: [:unix],
        rel_templates_path: "rel/control_plane",
        strip_beams: true
      ],
      favn_view: [
        version: {:from_app, :favn_orchestrator},
        applications: @view_applications,
        include_executables_for: [:unix],
        rel_templates_path: "rel/control_plane",
        strip_beams: true
      ]
    ]
  end
end
