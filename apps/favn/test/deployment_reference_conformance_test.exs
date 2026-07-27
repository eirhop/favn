defmodule Favn.DeploymentReferenceConformanceTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)

  test "Azure elastic job structurally follows the one-slot demand contract" do
    source = read("deployment/azure-container-apps/elastic-runner-job.bicep")
    resource = bicep_block(source, "resource runnerJob ")

    configuration = bicep_block(resource, "configuration:")
    scale = bicep_block(configuration, "scale:")
    template = bicep_block(resource, "template:")
    runner = bicep_block(template, "containers:")

    assert source =~ "resource runnerJob 'Microsoft.App/jobs@2026-01-01'"
    assert configuration =~ "triggerType: 'Event'"
    assert configuration =~ "replicaRetryLimit: 0"
    assert configuration =~ "parallelism: 1"
    assert configuration =~ "replicaCompletionCount: 1"
    assert scale =~ "minExecutions: 0"
    assert scale =~ "maxExecutions: maxExecutions"
    assert scale =~ "type: 'metrics-api'"
    assert scale =~ "valueLocation: 'outstanding'"
    assert scale =~ "targetValue: '1'"
    assert scale =~ "/internal/runner-demand/${runnerPool}/${runnerReleaseId}"
    assert runner =~ "name: 'FAVN_RUNNER_LIFECYCLE_MODE'"
    assert runner =~ "value: 'elastic'"
    assert runner =~ "name: 'FAVN_RUNNER_NODE_HOST_ALIAS'"
    assert runner =~ "name: 'FAVN_RUNNER_MAX_UPTIME_MS'"
  end

  test "Azure control plane has one replica and all mandatory production inputs" do
    source = read("deployment/azure-container-apps/control-plane.bicep")
    resource = bicep_block(source, "resource controlPlane ")
    configuration = bicep_block(resource, "configuration:")
    template = bicep_block(resource, "template:")
    control_plane = bicep_block(template, "containers:")
    scale = bicep_block(template, "scale:")

    assert source =~ "resource controlPlane 'Microsoft.App/containerApps@2026-01-01'"
    assert configuration =~ "external: false"
    assert configuration =~ "targetPort: 4369"
    assert configuration =~ "targetPort: 9100"
    assert scale =~ "minReplicas: 1"
    assert scale =~ "maxReplicas: 1"

    for variable <- [
          "FAVN_DEPLOYMENT_MODE",
          "FAVN_DATABASE_URL",
          "FAVN_DATABASE_SSL_MODE",
          "FAVN_DATABASE_SSL_CA_FILE",
          "FAVN_RUNTIME_INPUT_PIN_KEYS",
          "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION",
          "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
          "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME",
          "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD",
          "FAVN_WORKSPACE_IDS",
          "FAVN_RUNNER_POOLS",
          "FAVN_VIEW_PUBLIC_ORIGIN",
          "FAVN_VIEW_SECRET_KEY_BASE",
          "FAVN_VIEW_TRUSTED_PROXY_CIDRS",
          "FAVN_CONTROL_PLANE_NODE",
          "FAVN_DISTRIBUTION_COOKIE",
          "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE"
        ] do
      assert control_plane =~ "name: '#{variable}'"
    end

    assert configuration =~
             "capacity-scaler|capacity_reader:${capacityReaderToken}"

    assert control_plane =~ "-proto_dist inet_tls"
  end

  test "KEDA resources parse and encode exact authenticated default ScaledJob scaling" do
    documents =
      "deployment/kubernetes/elastic-runner.yaml"
      |> read()
      |> YamlElixir.read_all_from_string!()

    authentication = Enum.find(documents, &(&1["kind"] == "TriggerAuthentication"))
    scaled_job = Enum.find(documents, &(&1["kind"] == "ScaledJob"))

    assert get_in(authentication, ["spec", "secretTargetRef"]) == [
             %{"parameter" => "token", "name" => "favn-capacity-reader", "key" => "token"}
           ]

    spec = scaled_job["spec"]
    assert spec["minReplicaCount"] == 0
    assert spec["scalingStrategy"] == %{"strategy" => "default"}
    assert get_in(spec, ["jobTargetRef", "parallelism"]) == 1
    assert get_in(spec, ["jobTargetRef", "completions"]) == 1
    assert get_in(spec, ["jobTargetRef", "backoffLimit"]) == 0
    assert get_in(spec, ["jobTargetRef", "template", "spec", "restartPolicy"]) == "Never"

    [trigger] = spec["triggers"]
    assert trigger["type"] == "metrics-api"
    assert trigger["authenticationRef"] == %{"name" => "favn-capacity-reader"}
    assert trigger["metadata"]["valueLocation"] == "outstanding"
    assert trigger["metadata"]["targetValue"] == "1"
    assert trigger["metadata"]["authMode"] == "bearer"
    assert trigger["metadata"]["url"] =~ "/internal/runner-demand/POOL/RELEASE"
  end

  test "provider names and SDKs stay outside Favn core" do
    core =
      @root
      |> Path.join("apps/favn_core/lib/**/*.ex")
      |> Path.wildcard()
      |> Enum.map_join("\n", &File.read!/1)

    refute core =~ "Microsoft.App"
    refute core =~ "Kubernetes"
    refute core =~ "KEDA"
    refute core =~ "Azure SDK"
  end

  defp bicep_block(source, marker) do
    {marker_start, _length} = :binary.match(source, marker)
    tail = binary_part(source, marker_start, byte_size(source) - marker_start)
    {open_start, 1} = :binary.match(tail, "{")
    balanced(tail, open_start, open_start, 0)
  end

  defp balanced(source, start, cursor, depth) do
    case :binary.at(source, cursor) do
      ?{ ->
        balanced(source, start, cursor + 1, depth + 1)

      ?} when depth == 1 ->
        binary_part(source, start, cursor - start + 1)

      ?} ->
        balanced(source, start, cursor + 1, depth - 1)

      _other ->
        balanced(source, start, cursor + 1, depth)
    end
  end

  defp read(relative), do: @root |> Path.join(relative) |> File.read!()
end
