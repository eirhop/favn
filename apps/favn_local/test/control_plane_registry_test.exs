defmodule Favn.Dev.ControlPlaneRegistryTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../..", __DIR__)
  @script Path.join(@repo_root, "scripts/control_plane_registry.sh")
  @digest "sha256:" <> String.duplicate("a", 64)
  @other_digest "sha256:" <> String.duplicate("b", 64)

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_control_plane_registry_test_#{System.unique_integer([:positive])}"
      )

    bin = Path.join(root, "bin")
    File.mkdir_p!(bin)
    log = Path.join(root, "docker.log")
    count = Path.join(root, "inspect.count")

    write_executable!(Path.join(bin, "docker"), fake_docker())
    write_executable!(Path.join(bin, "gh"), fake_gh())

    on_exit(fn -> File.rm_rf(root) end)

    %{
      env: [
        {"PATH", bin <> ":" <> System.fetch_env!("PATH")},
        {"FAKE_DOCKER_LOG", log},
        {"FAKE_DOCKER_COUNT", count},
        {"FAKE_DIGEST", @digest}
      ],
      log: log
    }
  end

  test "registry lookups distinguish existing, missing, and failed requests", %{env: env} do
    assert {@digest <> "\n", 0} = run(["lookup-digest", "registry/image:tag"], env)

    assert {_output, 3} =
             run(["lookup-digest", "registry/image:missing"], [
               {"FAKE_DOCKER_MODE", "missing"} | env
             ])

    assert {output, 1} =
             run(["lookup-digest", "registry/image:error"], [
               {"FAKE_DOCKER_MODE", "error"} | env
             ])

    assert output =~ "registry lookup failed"
  end

  test "immutable aliases are reused, created once, and never overwritten", %{env: env, log: log} do
    assert {output, 0} =
             run(
               ["record-alias", "registry/image@#{@digest}", "registry/image:v1.0.0", @digest],
               env
             )

    assert output =~ "already points"
    refute File.read!(log) =~ "push"

    File.write!(log, "")

    assert {_output, 0} =
             run(
               ["record-alias", "registry/image@#{@digest}", "registry/image:v1.0.1", @digest],
               [{"FAKE_DOCKER_MODE", "missing_then_expected"} | env]
             )

    commands = File.read!(log)
    assert commands =~ "pull registry/image@#{@digest}"
    assert commands =~ "tag registry/image@#{@digest} registry/image:v1.0.1"
    assert commands =~ "push registry/image:v1.0.1"

    assert {output, 1} =
             run(
               ["record-alias", "registry/image@#{@digest}", "registry/image:v1.0.0", @digest],
               [{"FAKE_DOCKER_MODE", "mismatched"}, {"FAKE_OTHER_DIGEST", @other_digest} | env]
             )

    assert output =~ "refusing to overwrite"
  end

  test "GitHub release lookup accepts only an explicit 404", %{env: env} do
    assert {_output, 0} =
             run(
               ["require-github-release-absent", "eirhop/favn", "v1.0.0"],
               [{"FAKE_GH_HTTP", "404"} | env]
             )

    assert {output, 1} =
             run(
               ["require-github-release-absent", "eirhop/favn", "v1.0.0"],
               [{"FAKE_GH_HTTP", "200"} | env]
             )

    assert output =~ "already exists"

    assert {output, 1} =
             run(
               ["require-github-release-absent", "eirhop/favn", "v1.0.0"],
               [{"FAKE_GH_HTTP", "403"} | env]
             )

    assert output =~ "lookup failed"
  end

  test "workflows keep one toolchain and restrict pull-request permissions" do
    ci_workflow = File.read!(Path.join(@repo_root, ".github/workflows/ci.yml"))

    image_workflow =
      File.read!(Path.join(@repo_root, ".github/workflows/control-plane-image.yml"))

    release_workflow =
      File.read!(Path.join(@repo_root, ".github/workflows/control-plane-release.yml"))

    scheduled_scan_workflow =
      File.read!(Path.join(@repo_root, ".github/workflows/control-plane-security-scan.yml"))

    grype_policy = File.read!(Path.join(@repo_root, "security/control-plane-grype.yaml"))

    assert Regex.scan(
             ~r/otp-version: '([^']+)'/,
             ci_workflow <> image_workflow <> release_workflow,
             capture: :all_but_first
           )
           |> List.flatten()
           |> Enum.uniq() == ["29.0.3"]

    refute ci_workflow =~ "28.4.2"
    refute image_workflow =~ "28.4.2"
    refute release_workflow =~ "28.4.2"
    assert image_workflow =~ "mix local.hex 2.5.1 --force"
    refute ci_workflow =~ "mix local.hex --force"
    refute ci_workflow =~ "mix local.rebar --force"
    assert length(Regex.scan(~r/mix local\.hex 2\.5\.1 --force/, ci_workflow)) == 5

    assert length(
             Regex.scan(
               ~r{mix local\.rebar rebar3 https://github\.com/erlang/rebar3/releases/download/3\.27\.0/rebar3},
               ci_workflow
             )
           ) == 5

    assert length(Regex.scan(~r/grype-version: v0\.116\.0/, image_workflow)) == 4
    assert length(Regex.scan(~r/grype-version: v0\.116\.0/, release_workflow)) == 1

    assert length(Regex.scan(~r/config: security\/control-plane-grype\.yaml/, image_workflow)) ==
             4

    assert length(Regex.scan(~r/config: security\/control-plane-grype\.yaml/, release_workflow)) ==
             1

    assert length(Regex.scan(~r/only-fixed: false/, image_workflow)) == 4
    assert length(Regex.scan(~r/only-fixed: false/, release_workflow)) == 1
    refute image_workflow =~ "only-fixed: true"
    refute release_workflow =~ "only-fixed: true"

    assert length(
             Regex.scan(
               ~r/VULNERABILITY_EXCEPTION_REVIEW_BY: '2026-08-22'/,
               image_workflow
             )
           ) == 4

    assert release_workflow =~ "VULNERABILITY_EXCEPTION_REVIEW_BY: '2026-08-22'"
    assert image_workflow =~ "control_plane_qualification_id.exs"
    assert image_workflow =~ "runtime_qualification_changed"
    assert image_workflow =~ "security_scan_changed"
    assert image_workflow =~ "workflow_dispatch:"
    refute image_workflow =~ "workflow_run:"
    assert image_workflow =~ "Require current green main revision"
    assert image_workflow =~ ~s(gh run list --workflow CI --branch main --commit "$HEAD_SHA")

    assert image_workflow =~
             "Manual publication always runs complete runtime and security qualification."

    [discover_job, _rest] = String.split(image_workflow, "  pr-candidate:", parts: 2)
    assert discover_job =~ "Enforce vulnerability exception review deadline"
    assert discover_job =~ "VULNERABILITY_EXCEPTION_REVIEW_BY: '2026-08-22'"

    assert grype_policy =~ "Review by: 2026-08-22"
    assert length(Regex.scan(~r/^  - vulnerability:/m, grype_policy)) == 26
    assert length(Regex.scan(~r/^    fix-state: (?:not-fixed|wont-fix)$/m, grype_policy)) == 26
    assert length(Regex.scan(~r/^      name: /m, grype_policy)) == 26
    refute grype_policy =~ "ignore-wontfix"
    assert image_workflow =~ "control_plane_registry.sh record-alias"
    assert image_workflow =~ ~s($IMAGE_REPOSITORY:sha-$HEAD_SHA)
    assert Bitwise.band(File.stat!(@script).mode, 0o111) != 0
    assert image_workflow =~ ~s(staging-$HEAD_SHA-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT)
    refute image_workflow =~ ~s(docker tag "$candidate" "$build_ref")

    [pr_job, _rest] = String.split(image_workflow, "  main-image:", parts: 2)
    [_before, pr_job] = String.split(pr_job, "  pr-candidate:", parts: 2)
    assert pr_job =~ "permissions:\n      contents: read"
    refute pr_job =~ "packages: write"
    assert pr_job =~ "pr-runtime-acceptance:"
    assert pr_job =~ "runtime_qualification_changed == 'true'"
    assert pr_job =~ "pr-security-scan:"
    assert pr_job =~ "security_scan_changed == 'true'"
    assert pr_job =~ "Scan resolved control plane"
    refute image_workflow =~ "apps/favn_azure/*"
    assert pr_job =~ ~s(ref="$IMAGE_REPOSITORY:build-$BUILD_ID")

    [runtime_job, security_job] =
      pr_job
      |> String.split("  pr-runtime-acceptance:", parts: 2)
      |> List.last()
      |> String.split("  pr-security-scan:", parts: 2)

    refute runtime_job =~ "anchore/scan-action@"
    assert security_job =~ "anchore/scan-action@"

    assert pr_job =~
             ~s(require-digest "$IMAGE_REPOSITORY:verified-build-$BUILD_ID" "$digest")

    assert pr_job =~
             ~s(control_plane_image_contract.sh "$IMAGE_REPOSITORY@$digest" "$BUILD_ID")

    assert pr_job =~ "FAVN_CONTROL_PLANE_CANDIDATE: ${{ steps.image.outputs.reference }}"
    assert length(Regex.scan(~r/Resolve verified or unpublished control plane/, pr_job)) == 2
    assert pr_job =~ "MIX_ENV=prod mix favn.build.control_plane --load"
    assert pr_job =~ "elif [[ $lookup_status -eq 3 ]]"

    assert image_workflow =~
             "needs: [discover, pr-candidate, pr-runtime-acceptance, pr-security-scan, main-image, verify-published, record-main-verification]"

    assert image_workflow =~ "PR_RUNTIME_RESULT: ${{ needs.pr-runtime-acceptance.result }}"
    assert image_workflow =~ "PR_SECURITY_RESULT: ${{ needs.pr-security-scan.result }}"
    assert image_workflow =~ ~s([[ "$PR_RUNTIME_RESULT" == success ]])
    assert image_workflow =~ ~s([[ "$PR_SECURITY_RESULT" == success ]])

    [main_job, _rest] =
      image_workflow
      |> String.split("  main-image:", parts: 2)
      |> List.last()
      |> String.split("  record-main-verification:", parts: 2)

    assert main_job =~ "- name: Run representative pre-publish runtime acceptance"

    refute main_job =~
             "- name: Compile production container acceptance tests\n        if: steps.lookup.outputs.exists == 'false'"

    refute main_job =~
             "- name: Run representative pre-publish runtime acceptance\n        if: steps.lookup.outputs.exists == 'false'"

    assert main_job =~ "Scan reused official control plane"
    assert main_job =~ "needs.discover.outputs.security_scan_changed == 'true'"
    assert main_job =~ ~s(image="$IMAGE_REPOSITORY@${{ steps.lookup.outputs.digest }}")

    [record_job, _rest] =
      image_workflow
      |> String.split("  record-main-verification:", parts: 2)
      |> List.last()
      |> String.split("  control-plane-image:", parts: 2)

    assert record_job =~ "actions/checkout@"
    assert record_job =~ "docker/setup-buildx-action@"
    assert record_job =~ ~s($IMAGE_REPOSITORY:build-$BUILD_ID)
    assert record_job =~ "control_plane_registry.sh record-alias"

    assert record_job =~
             "[[ $(git rev-parse --verify 'origin/main^{commit}') == \"$HEAD_SHA\" ]]"

    assert release_workflow =~ "require-github-release-absent"
    assert release_workflow =~ "require-digest \"$IMAGE_REPOSITORY:sha-$TAGGED_SHA\""
    assert release_workflow =~ "record-alias"
    assert scheduled_scan_workflow =~ "cron: '17 4 * * *'"
    assert scheduled_scan_workflow =~ "Scan current verified image"
    assert scheduled_scan_workflow =~ "verified-build-$BUILD_ID"
    assert scheduled_scan_workflow =~ "Verify build provenance"
    assert scheduled_scan_workflow =~ "grype-version: v0.116.0"
  end

  test "new image publication exposes no immutable cache key before qualification" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/control-plane-image.yml"))

    staging = byte_offset!(workflow, "Push run-scoped staging tag")
    provenance = byte_offset!(workflow, "Attest build provenance")
    sbom = byte_offset!(workflow, "Attest SBOM")
    clean_verification = byte_offset!(workflow, "Verify published digest cleanly")
    immutable_alias = byte_offset!(workflow, ~s(build_ref="$IMAGE_REPOSITORY:build-$BUILD_ID"))

    assert staging < provenance
    assert provenance < sbom
    assert sbom < clean_verification
    assert clean_verification < immutable_alias

    before_clean_verification = binary_part(workflow, 0, clean_verification)

    refute before_clean_verification =~
             "record-alias \"$IMAGE_REPOSITORY@$DIGEST\" \"$build_ref\""
  end

  defp run(arguments, env) do
    System.cmd("bash", [@script | arguments], env: env, stderr_to_stdout: true)
  end

  defp byte_offset!(source, pattern) do
    {offset, _length} = :binary.match(source, pattern)
    offset
  end

  defp write_executable!(path, source) do
    File.write!(path, source)
    File.chmod!(path, 0o755)
  end

  defp fake_docker do
    """
    #!/usr/bin/env bash
    set -euo pipefail
    printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"

    if [[ "${1:-}" == buildx ]]; then
      mode=${FAKE_DOCKER_MODE:-existing}

      case "$mode" in
        existing)
          printf 'Digest: %s\n' "$FAKE_DIGEST"
          ;;
        missing)
          echo 'manifest unknown' >&2
          exit 1
          ;;
        error)
          echo 'unauthorized' >&2
          exit 1
          ;;
        mismatched)
          printf 'Digest: %s\n' "$FAKE_OTHER_DIGEST"
          ;;
        missing_then_expected)
          count=0
          if [[ -f "$FAKE_DOCKER_COUNT" ]]; then count=$(cat "$FAKE_DOCKER_COUNT"); fi
          count=$((count + 1))
          printf '%s' "$count" > "$FAKE_DOCKER_COUNT"
          if [[ $count -eq 1 ]]; then
            echo 'manifest unknown' >&2
            exit 1
          fi
          printf 'Digest: %s\n' "$FAKE_DIGEST"
          ;;
      esac
    fi
    """
  end

  defp fake_gh do
    """
    #!/usr/bin/env bash
    set -euo pipefail
    status=${FAKE_GH_HTTP:-404}
    printf 'HTTP/2.0 %s status\n\n' "$status"
    if [[ "$status" == 200 ]]; then exit 0; fi
    exit 1
    """
  end
end
