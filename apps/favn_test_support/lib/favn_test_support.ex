defmodule FavnTestSupport do
  @moduledoc """
  Shared test support infrastructure for umbrella apps.

  This app owns cross-app fixture source files and small helper APIs that keep
  test fixture loading deterministic and dependency-light.
  """

  @doc "Returns a canonical deterministic runner release ID for tests."
  @spec runner_release_id(:primary | :alternate) :: String.t()
  def runner_release_id(name \\ :primary) when name in [:primary, :alternate] do
    digest =
      :crypto.hash(:sha256, "favn-test-runner-" <> Atom.to_string(name))
      |> Base.encode16(case: :lower)

    "rr_" <> digest
  end

  @doc """
  Returns a deterministic, validated runner identity for tests.

  The constructor is invoked dynamically to preserve `favn_test_support`'s
  dependency-light compile boundary.
  """
  @spec runner_release(:primary | :alternate) :: struct()
  def runner_release(name \\ :primary) when name in [:primary, :alternate] do
    attrs = %{
      favn_version: apply(Favn.RunnerRelease, :current_favn_version, []),
      runner_contract_version:
        apply(Favn.Manifest.Compatibility, :current_runner_contract_version, []),
      elixir_version: System.version(),
      otp_release: :erlang.system_info(:otp_release) |> to_string(),
      target: apply(Favn.RunnerRelease, :current_target, []),
      runner_release_id: runner_release_id(name),
      build_profile: "prod"
    }

    {:ok, runner_release} = apply(Favn.RunnerRelease, :new, [attrs])
    runner_release
  end

  @doc """
  Adds the current schema, runner contract, and required runner release ID to a
  manifest fixture.

  Version functions are invoked dynamically so this dependency-light support
  app does not create a compile-time dependency cycle with `favn_core`.
  """
  @spec with_manifest_contract(map(), String.t()) :: map()
  def with_manifest_contract(manifest, runner_release_id \\ runner_release_id())
      when is_map(manifest) and is_binary(runner_release_id) do
    manifest =
      if Map.has_key?(manifest, :assets) do
        Map.update!(manifest, :assets, fn assets ->
          Enum.map(assets, &with_semantic_generation(&1, runner_release_id))
        end)
      else
        manifest
      end

    manifest
    |> Map.merge(%{
      schema_version: apply(Favn.Manifest.Compatibility, :current_schema_version, []),
      runner_contract_version:
        apply(Favn.Manifest.Compatibility, :current_runner_contract_version, []),
      required_runner_release_id: runner_release_id
    })
  end

  defp with_semantic_generation(%{target_descriptor: descriptor} = asset, _runner_release_id)
       when not is_nil(descriptor),
       do: asset

  defp with_semantic_generation(%{ref: {module, name}} = asset, runner_release_id)
       when is_atom(module) and is_atom(name) do
    asset_value = if is_struct(asset), do: Map.from_struct(asset), else: asset

    generation_id =
      apply(Favn.Manifest.TargetDescriptor, :semantic_generation_id, [
        asset_value,
        runner_release_id
      ])

    Map.put(asset, :semantic_generation_id, generation_id)
  end

  defp with_semantic_generation(asset, _runner_release_id), do: asset

  @doc """
  Adds a canonical target descriptor to a persisted SQL manifest asset fixture.

  Descriptor construction is invoked dynamically to preserve
  `favn_test_support`'s dependency-light compile boundary.
  """
  @spec with_target_descriptor(struct()) :: struct()
  def with_target_descriptor(%{relation: %{connection: connection}} = asset) do
    schema_version = apply(Favn.Manifest.Compatibility, :current_schema_version, [])

    runner_contract_version =
      apply(Favn.Manifest.Compatibility, :current_runner_contract_version, [])

    descriptor =
      apply(Favn.Manifest.TargetDescriptor, :from_asset, [
        asset,
        [
          connection_definitions: %{
            connection => %{adapter: FavnTestSupport.TargetAdapter, module: nil}
          },
          manifest_schema_version: schema_version,
          runner_contract_version: runner_contract_version
        ]
      ])

    Map.put(asset, :target_descriptor, descriptor)
  end

  @doc """
  Adds the canonical dependency graph to a manifest fixture.

  The graph builder is invoked dynamically so this dependency-light support
  app can serve `favn_core` tests without creating an umbrella dependency
  cycle.
  """
  @spec with_manifest_graph(map()) :: map()
  def with_manifest_graph(%{assets: assets} = manifest) when is_list(assets) do
    {:ok, graph} = apply(Favn.Manifest.Graph, :build, [assets])
    Map.put(manifest, :graph, graph)
  end
end
