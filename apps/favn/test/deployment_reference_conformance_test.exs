defmodule Favn.DeploymentReferenceConformanceTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)

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
    certificates = services["certificates"]
    control_plane = services["control-plane"]
    runner = services["runner"]
    scaler_service = services["scaler"]
    qualification_service = services["qualification"]

    assert certificates["image"] == "favn-qualification-certificates:${FAVN_IMAGE_TAG}"
    assert postgres["image"] == "favn-qualification-postgres:${FAVN_IMAGE_TAG}"
    assert "ssl=on" in postgres["command"]
    assert "log_parameter_max_length=0" in postgres["command"]
    assert "log_parameter_max_length_on_error=0" in postgres["command"]
    assert postgres["healthcheck"]["test"] |> List.last() =~ "sslmode=verify-full"
    assert postgres["environment"]["POSTGRES_USER"] == "favn_bootstrap"

    database_bootstrap = services["database-bootstrap"]
    assert database_bootstrap["command"] == ["bootstrap"]
    assert database_bootstrap["profiles"] == ["operations"]
    assert database_bootstrap["restart"] == "no"

    assert database_bootstrap["environment"]["FAVN_DATABASE_BOOTSTRAP_URL"] =~
             "favn_bootstrap:"

    assert database_bootstrap["environment"]["FAVN_DATABASE_MIGRATOR_URL"] =~
             "favn_migrator:"

    assert database_bootstrap["environment"]["FAVN_DATABASE_RUNTIME_URL"] =~
             "favn_runtime:"

    refute Map.has_key?(services, "database-migrate")
    refute Map.has_key?(services, "database-grant")
    refute Map.has_key?(services, "database-verify")

    assert get_in(control_plane, ["build", "args", "FAVN_CONTROL_PLANE_VERSION"]) ==
             "${FAVN_CONTROL_PLANE_VERSION}"

    assert get_in(control_plane, ["build", "args", "FAVN_MANIFEST_SCHEMA_VERSION"]) ==
             "${FAVN_MANIFEST_SCHEMA_VERSION}"

    assert get_in(control_plane, ["build", "args", "FAVN_RUNNER_CONTRACT_VERSION"]) ==
             "${FAVN_RUNNER_CONTRACT_VERSION}"

    assert control_plane["environment"]["FAVN_DEPLOYMENT_MODE"] == "production"
    assert control_plane["environment"]["FAVN_DATABASE_SSL_MODE"] == "verify-full"
    refute Map.has_key?(control_plane["environment"], "FAVN_DATABASE_BOOTSTRAP_URL")
    refute Map.has_key?(control_plane["environment"], "FAVN_DATABASE_MIGRATOR_URL")

    assert control_plane["environment"]["FAVN_VIEW_TRUSTED_PROXY_CIDRS"] ==
             "172.31.58.2/32"

    assert control_plane["environment"]["FAVN_VIEW_FORWARDED_FOR_POLICY"] == "replace"

    assert control_plane["environment"]["FAVN_DISTRIBUTION_TLS_OPTIONS_FILE"] ==
             "/etc/favn/tls/control-plane-ssl-dist.config"

    assert control_plane["environment"]["ERL_AFLAGS"] =~ "-proto_dist inet_tls"
    assert control_plane["environment"]["FAVN_CONTROL_PLANE_NODE"] =~ ".favn.local"
    assert control_plane["environment"]["FAVN_RUNNER_POOLS"] =~ ~s("mode":"elastic")
    assert control_plane["environment"]["FAVN_RUNNER_POOLS"] =~ ~s("idle_grace_ms":5000)

    assert control_plane["environment"]["FAVN_ORCHESTRATOR_API_SERVICE_TOKENS"] =~
             "platform-v1|platform_operator+platform_reader:"

    refute control_plane["environment"]["FAVN_ORCHESTRATOR_API_SERVICE_TOKENS"] =~
             "capacity_reader"

    assert control_plane["environment"]["FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN"] ==
             "${FAVN_CAPACITY_TOKEN}"

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

    https_proxy = services["https-proxy"]
    proxy_security = services["proxy-security"]

    assert https_proxy["profiles"] == ["proxy-security"]
    assert get_in(https_proxy, ["networks", "favn-edge", "ipv4_address"]) == "172.31.58.2"
    assert proxy_security["profiles"] == ["proxy-security"]
    assert get_in(proxy_security, ["networks", "favn-edge", "ipv4_address"]) == "172.31.58.4"

    assert get_in(services, ["proxy-header-receiver", "networks", "favn-edge", "ipv4_address"]) ==
             "172.31.58.5"

    assert get_in(services, ["proxy-header-receiver", "entrypoint"]) == ["nc"]

    assert get_in(services, ["proxy-header-receiver", "command"]) ==
             ["-l", "-p", "8080", "-e", "/usr/local/bin/receive-proxy-headers"]

    caddy = read("deployment/docker-compose/trusted-proxy.Caddyfile")
    assert caddy =~ "/__proxy_header_probe"
    assert caddy =~ "header_up -Forwarded"
    assert caddy =~ "header_up X-Forwarded-For {remote_host}"
    assert caddy =~ "header_up X-Forwarded-Proto https"

    proxy_check = read("deployment/docker-compose/check-trusted-proxy.sh")
    assert proxy_check =~ "TP-001"
    assert proxy_check =~ "TP-002"
    assert proxy_check =~ "TP-003"
    assert proxy_check =~ "TP-004"
    assert proxy_check =~ "proxy-upstream-request.txt"
    assert proxy_check =~ "X-Forwarded-For: 172\\.31\\.58\\.4"
    assert proxy_check =~ "X-Forwarded-Proto: https"
    assert proxy_check =~ "https://favn.localhost/api/web/v1/health/ready"

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
    assert qualification =~ "FAVN_BOOTSTRAP_DATABASE_PASSWORD"
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

  test "local PostgreSQL uses the same bootstrap role separation" do
    compose = "compose.postgres.yml" |> read() |> YamlElixir.read_from_string!()
    postgres = compose["services"]["postgres"]
    setup = read("scripts/postgres/setup")
    reset = read("scripts/postgres/reset")

    assert postgres["environment"]["POSTGRES_USER"] == "favn_bootstrap"
    assert "log_parameter_max_length=0" in postgres["command"]
    assert "log_parameter_max_length_on_error=0" in postgres["command"]

    refute Enum.any?(postgres["volumes"], &String.contains?(&1, "docker-entrypoint-initdb.d"))

    assert setup =~ "FAVN_DATABASE_BOOTSTRAP_URL"
    assert setup =~ "FAVN_DATABASE_MIGRATOR_URL"
    assert setup =~ "FAVN_DATABASE_RUNTIME_URL"
    assert setup =~ "FavnStoragePostgres.ReleaseCLI.run!(:bootstrap)"
    refute setup =~ "mix favn.postgres.upgrade"
    assert reset =~ "exec scripts/postgres/setup"
  end

  test "production operations wrapper exposes only composed database lifecycle commands" do
    wrapper = read("rel/control_plane/overlays/bin/favn_control_plane_ops")

    assert wrapper =~ "bootstrap) operation=bootstrap"
    assert wrapper =~ "status) operation=status"
    assert wrapper =~ "upgrade) operation=upgrade"
    refute wrapper =~ "migrate) operation=migrate"
    refute wrapper =~ "grant-runtime) operation=grant_runtime"
    refute wrapper =~ "provision-workspace) operation=provision_workspace"
    refute wrapper =~ "verify-schema) operation=verify_schema"
  end

  test "Compose qualification reuses revision images and excludes generated state" do
    prepare = read("deployment/docker-compose/prepare.sh")
    assert prepare =~ "FAVN_IMAGE_BUILD_PROJECT_NAME"
    assert prepare =~ "FAVN_IMAGE_BUILDER_NAME"
    assert prepare =~ "git -C \"$repository_root\" show -s --format=%cI"
    assert prepare =~ "Docker qualification requires a clean checkout"
    assert prepare =~ "dirty diagnostic builds require an explicit unique FAVN_IMAGE_TAG"
    assert prepare =~ "favn-compose-runner:$FAVN_SOURCE_REVISION:$FAVN_IMAGE_TAG"
    assert prepare =~ "release_metadata=$(\"$repository_root/scripts/release_metadata.sh\")"
    assert prepare =~ "\"FAVN_CONTROL_PLANE_VERSION=$FAVN_CONTROL_PLANE_VERSION\""
    assert prepare =~ "\"FAVN_MANIFEST_SCHEMA_VERSION=$FAVN_MANIFEST_SCHEMA_VERSION\""
    assert prepare =~ "\"FAVN_RUNNER_CONTRACT_VERSION=$FAVN_RUNNER_CONTRACT_VERSION\""
    assert prepare =~ "sha256sum"
    assert prepare =~ "\"FAVN_PLATFORM_TOKEN=$(random_hex 48)\""
    assert prepare =~ "\"FAVN_CAPACITY_TOKEN=$(random_hex 48)\""

    security_compose = read("deployment/docker-compose/compose.security.yml")

    assert length(
             Regex.scan(~r/image: favn-security-probe:\$\{FAVN_IMAGE_TAG\}/, security_compose)
           ) ==
             2

    assert length(Regex.scan(~r/context: \.\/security/, security_compose)) == 1

    for path <- [
          "deployment/docker-compose/run-simulation.sh",
          "deployment/docker-compose/run-trusted-proxy-security.sh",
          "deployment/docker-compose/run-security-qualification.sh"
        ] do
      runner = read(path)
      assert runner =~ "--project-name \"$image_build_project_name\""
      assert runner =~ "verify-image-source.sh"
      assert runner =~ "ensure-image-builder.sh"
      assert runner =~ "--builder \"$image_builder_name\""
      assert runner =~ "--provenance=false"
      assert runner =~ "BUILDX_NO_DEFAULT_ATTESTATIONS=1"
      assert runner =~ "build_compose build --help"
    end

    security_runner = read("deployment/docker-compose/run-security-qualification.sh")
    assert security_runner =~ "certificates postgres control-plane security-browser"
    refute security_runner =~ "control-plane security-browser security-api"

    for path <- [
          "rel/control_plane/Dockerfile.dockerignore",
          "deployment/docker-compose/customer.Dockerfile.dockerignore"
        ] do
      dockerignore = read(path)
      assert dockerignore =~ "deployment/docker-compose/security/secrets"
      assert dockerignore =~ "deployment/docker-compose/security-results"
    end

    assert read("deployment/docker-compose/security/Dockerfile.dockerignore") ==
             "**\n!package.json\n!package-lock.json\n"

    local_context_ignore = read("deployment/docker-compose/.dockerignore")

    for source <- [
          "generate-certificates.sh",
          "postgres-entrypoint.sh",
          "scale-runners.sh",
          "qualify-postgres.sh",
          "qualification-observe.sql",
          "qualification-outcomes.sql"
        ] do
      assert local_context_ignore =~ "!#{source}"
    end

    buildkit_config = read("deployment/docker-compose/buildkitd.toml")
    assert buildkit_config =~ ~s(maxUsedSpace = "12GB")
    assert buildkit_config =~ ~s(minFreeSpace = "20GB")

    builder = read("deployment/docker-compose/ensure-image-builder.sh")
    assert builder =~ "--driver docker-container"
    assert builder =~ "--driver-opt default-load=true"
    assert builder =~ "--buildkitd-config"
    assert builder =~ "maxUsedSpace = '12GB'"
    assert builder =~ "minFreeSpace = '20GB'"

    source_verifier = read("deployment/docker-compose/verify-image-source.sh")
    assert source_verifier =~ "prepared image revision does not match the checked-out HEAD"
    assert source_verifier =~ "qualification images require the unchanged clean revision"
    assert source_verifier =~ "dirty qualification requires a unique diagnostic image tag"

    image_pruner = read("deployment/docker-compose/prune-qualification-images.sh")
    assert image_pruner =~ "^pr565-[0-9a-f]{12}$"
    assert image_pruner =~ "^diagnostic-[0-9a-f]{12}-[0-9]{14}-[0-9]+$"
    assert image_pruner =~ "protected_clean_tag"
    assert image_pruner =~ ~s([ "$retained_clean_tags" -le 3 ])
    refute image_pruner =~ "docker image prune"

    cleanup = read("deployment/docker-compose/cleanup.sh")
    assert cleanup =~ "--rmi local"
    assert cleanup =~ "prune-qualification-images.sh"

    security_runner = read("deployment/docker-compose/run-security-qualification.sh")
    assert security_runner =~ "FAVN_IMAGE_TAG=\"diagnostic-$short_revision-$run_suffix\""
    assert security_runner =~ "prune-qualification-images.sh"
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

  defp read(relative), do: @root |> Path.join(relative) |> File.read!()
end
