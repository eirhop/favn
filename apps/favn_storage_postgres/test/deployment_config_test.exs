defmodule FavnStoragePostgres.DeploymentConfigTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Environment
  alias FavnOrchestrator.ExecutionPoolPolicy
  alias FavnStoragePostgres.DeploymentConfig

  test "accepts one resolved immutable execution-pool policy snapshot" do
    assert {:ok, resolved} = resolved_policy()
    assert {:ok, resolved.configuration} == DeploymentConfig.validate(resolved.configuration)
  end

  test "rejects policy content whose fingerprint or provenance was changed" do
    assert {:ok, resolved} = resolved_policy()

    changed_limit =
      put_in(
        resolved.configuration,
        ["execution_pool_policy", "effective", "api", "max_concurrency"],
        99
      )

    assert {:error, :execution_pool_policy_fingerprint_mismatch} =
             DeploymentConfig.validate(changed_limit)

    changed_source =
      put_in(
        resolved.configuration,
        ["execution_pool_policy", "sources", "api"],
        "manifest"
      )

    assert {:error, :invalid_execution_pool_policy_sources} =
             DeploymentConfig.validate(changed_source)
  end

  test "rejects atom and string spellings of the same configuration key" do
    assert {:error, :duplicate_configuration_keys} =
             DeploymentConfig.validate(%{"resources" => %{}, resources: %{}})
  end

  test "version two requires a validated workspace environment" do
    environment = Environment.new!(default_timezone: "Europe/Oslo")

    assert {:ok, configuration} =
             FavnOrchestrator.WorkspaceConfiguration.put(%{"resources" => %{}}, %Manifest{
               environment: environment
             })

    assert {:ok, ^configuration} = DeploymentConfig.validate(configuration)

    assert {:error, :workspace_environment_required} =
             DeploymentConfig.validate(%{"schema_version" => 2, "resources" => %{}})

    assert {:error, {:workspace_environment_requires_schema_version, 2}} =
             DeploymentConfig.validate(%{
               "schema_version" => 1,
               "resources" => %{},
               "workspace_environment" => %{"arbitrary" => "json"}
             })

    invalid =
      put_in(
        configuration,
        ["workspace_environment", "environment", "default_timezone"],
        "Invalid/Timezone"
      )

    assert {:error, {:invalid_timezone, "Invalid/Timezone"}} =
             DeploymentConfig.validate(invalid)
  end

  test "workspace environment normalizes an atom schema key before validation" do
    environment = Environment.new!(default_timezone: "Europe/Oslo")

    assert {:ok, configuration} =
             FavnOrchestrator.WorkspaceConfiguration.put(
               %{schema_version: 1, resources: %{}},
               %Manifest{environment: environment}
             )

    refute Map.has_key?(configuration, :schema_version)
    assert configuration["schema_version"] == 2
    assert {:ok, ^configuration} = DeploymentConfig.validate(configuration)
  end

  test "the largest accepted pool catalogue and override set fits durable configuration" do
    defaults =
      Map.new(1..Favn.ExecutionPool.PolicySet.maximum_pools(), fn index ->
        name = "pool#{index}" |> String.pad_trailing(63, "x")

        {name,
         %{
           max_concurrency: index,
           circuit_breaker: %{failure_threshold: 10_000, probe_after_ms: 86_400_000}
         }}
      end)

    overrides =
      Map.new(defaults, fn {name, policy} ->
        {name, %{policy | max_concurrency: policy.max_concurrency + 1}}
      end)

    assert {:ok, resolved} =
             ExecutionPoolPolicy.resolve(
               %Manifest{execution_pools: defaults},
               %{},
               %{},
               %{approve_manifest_defaults: true, overrides: overrides}
             )

    assert byte_size(Jason.encode!(resolved.configuration)) <= 262_144
    assert {:ok, resolved.configuration} == DeploymentConfig.validate(resolved.configuration)
  end

  defp resolved_policy do
    ExecutionPoolPolicy.resolve(
      %Manifest{execution_pools: %{api: %{max_concurrency: 3}}},
      %{},
      %{},
      %{
        approve_manifest_defaults: true,
        overrides: %{api: %{max_concurrency: 4}}
      }
    )
  end
end
