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

  test "Compose qualification uses production TLS and one-shot elastic runners" do
    source = read("deployment/docker-compose/compose.yml")
    compose = YamlElixir.read_from_string!(source)
    services = compose["services"]

    postgres = services["postgres"]
    control_plane = services["control-plane"]
    runner = services["runner"]
    scaler_service = services["scaler"]
    qualification_service = services["qualification"]

    assert "ssl=on" in postgres["command"]
    assert postgres["healthcheck"]["test"] |> List.last() =~ "sslmode=verify-full"
    assert get_in(services, ["database-migrate", "command"]) == ["migrate"]
    assert get_in(services, ["database-grant", "command"]) == ["grant-runtime"]
    assert get_in(services, ["database-verify", "command"]) == ["verify-schema"]

    assert control_plane["environment"]["FAVN_DEPLOYMENT_MODE"] == "production"
    assert control_plane["environment"]["FAVN_DATABASE_SSL_MODE"] == "verify-full"

    assert control_plane["environment"]["FAVN_DISTRIBUTION_TLS_OPTIONS_FILE"] ==
             "/etc/favn/tls/control-plane-ssl-dist.config"

    assert control_plane["environment"]["ERL_AFLAGS"] =~ "-proto_dist inet_tls"
    assert control_plane["environment"]["FAVN_CONTROL_PLANE_NODE"] =~ ".favn.local"
    assert control_plane["environment"]["FAVN_RUNNER_POOLS"] =~ ~s("mode":"elastic")
    assert control_plane["environment"]["FAVN_RUNNER_POOLS"] =~ ~s("idle_grace_ms":5000)

    assert control_plane["environment"]["FAVN_ORCHESTRATOR_API_SERVICE_TOKENS"] =~
             "platform-v1|platform_operator+platform_reader:"

    assert "control-plane-certificates:/etc/favn/tls:ro" in control_plane["volumes"]
    refute Enum.any?(control_plane["volumes"], &String.starts_with?(&1, "runner-certificates:"))

    assert runner["profiles"] == ["runner"]
    assert runner["restart"] == "no"
    assert runner["environment"]["FAVN_RUNNER_LIFECYCLE_MODE"] == "elastic"
    assert runner["environment"]["FAVN_RUNNER_POOL"] == "default"
    assert runner["environment"]["FAVN_RUNNER_RELEASE_ID"] == "${FAVN_RUNNER_RELEASE_ID}"
    assert runner["environment"]["ERL_AFLAGS"] =~ "-proto_dist inet_tls"
    assert runner["hostname"] == "${FAVN_RUNNER_NODE_HOST_ALIAS:-runner.favn.local}"
    assert runner["volumes"] == ["runner-certificates:/etc/favn/tls:ro"]

    assert scaler_service["profiles"] == ["scaler"]
    assert "/var/run/docker.sock:/var/run/docker.sock" in scaler_service["volumes"]

    assert qualification_service["profiles"] == ["qualification"]
    assert qualification_service["entrypoint"] == ["/usr/local/bin/qualify-favn-postgres"]
    assert "/var/run/docker.sock:/var/run/docker.sock" in qualification_service["volumes"]

    assert "postgres-certificates:/etc/favn/postgres-tls:ro" in qualification_service[
             "volumes"
           ]

    assert qualification_service["environment"]["FAVN_QUALIFICATION_DURATION_SECONDS"] ==
             "${FAVN_QUALIFICATION_DURATION_SECONDS:-14400}"

    scaler = read("deployment/docker-compose/scale-runners.sh")
    assert scaler =~ "outstanding - running"
    assert scaler =~ "compose run"
    assert scaler =~ "--detach"
    assert scaler =~ "--no-deps"
    assert scaler =~ "FAVN_RUNNER_NODE_HOST_ALIAS=$container_name"
    assert scaler =~ "FAVN_SCALER_MAX_LAUNCHES"
    assert scaler =~ "exited with code"
    assert scaler =~ "0 -> 3 -> 2 -> 1 -> 0"
    assert scaler =~ "unexpected runner transition"
    assert scaler =~ "FAVN_SCALER_ALLOWED_FAILURES_FILE"
    assert scaler =~ "FAVN_SCALER_DRAIN_SIGNAL_FILE"
    assert scaler =~ "snapshot_api_unavailable"
    assert scaler =~ "active_launched_ids"
    refute scaler =~ "compose up --scale"
    refute scaler =~ "docker stop"

    simulation = read("deployment/docker-compose/run-simulation.sh")
    assert simulation =~ "for workload in fast medium slow"
    assert simulation =~ "operator \"run-$workload\""
    assert simulation =~ "FAVN_MANIFEST_VERSION_ID=$manifest_version_id"
    assert simulation =~ ~s([ "${#digest}" -eq 64 ])
    assert simulation =~ "scaler did not exit within 120 seconds"

    qualification = read("deployment/docker-compose/qualify-postgres.sh")
    assert qualification =~ "/api/orchestrator/v1/runs"
    assert qualification =~ "Idempotency-Key: $idempotency_key"
    assert qualification =~ "idempotency_conflict"
    assert qualification =~ "docker kill --signal KILL"
    assert qualification =~ "inject_service_failure control-plane"
    assert qualification =~ "inject_service_failure postgres"
    assert qualification =~ "qualification-observe.sql"
    assert qualification =~ "qualification-outcomes.sql"
    assert qualification =~ "safe_failure_classification"
    assert qualification =~ "non_reusable_materialization_claim_succeeded"
    assert qualification =~ "final-validation.json"
    assert qualification =~ "secret_scan_expected=8"
    assert qualification =~ "configured_secret_values_scanned"
    assert qualification =~ "leaked_variable_names"
    refute qualification =~ "mix favn.run"

    qualification_runner = read("deployment/docker-compose/run-qualification.sh")
    assert qualification_runner =~ "run-simulation.sh"
    assert qualification_runner =~ "compose run"
    assert qualification_runner =~ "--detach"
    assert qualification_runner =~ "FAVN_QUALIFICATION_RUN_ID=$run_id"

    status = read("deployment/docker-compose/qualification-status.sh")
    assert status =~ "controller.log"
    assert status =~ "final-validation.json"
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
