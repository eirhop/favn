# Control-plane image

Favn publishes one Linux amd64 control-plane image at
`ghcr.io/eirhop/favn-control-plane`. It runs Favn View, Orchestrator,
PostgreSQL storage, and Core in one `favn_control_plane` BEAM. Customer code,
the runner, local tooling, Mix, source files, and build caches are not present in
the runtime image.

Deploy the image only by repository-qualified digest:

```text
ghcr.io/eirhop/favn-control-plane@sha256:<64 lowercase hexadecimal characters>
```

Tags are lookup aliases, not deployment identities. A maintainer deliberately
dispatches image publication only after the current `main` revision has passed
CI and is stable. That workflow writes immutable
`build-<control_plane_build_id>` tags, then writes
`verified-build-<control_plane_build_id>` and `sha-<git_sha>` only after the
selected revision and any unverified digest pass clean verification. Ordinary
merges never publish an image. Pushing a semantic `v<favn_version>` Git tag
starts release qualification. The workflow adds that version alias to the
already-built digest and only then publishes the GitHub Release with its
deployment reference; it does not rebuild the image. A release that changes
only runner or development tooling therefore aliases the previous compatible
control-plane digest.

## Registry setup and access

The workflows use the repository `GITHUB_TOKEN`; no long-lived publisher
credential is required. Authority is scoped by event and job:

| Event | Expensive work | Registry/repository authority | Required result |
| --- | --- | --- | --- |
| Pull request to `main` | Build, inspect, scan, and create an SBOM only when the control-plane identity changes; otherwise run only changed runtime-compatibility or scan qualification | `contents: read`; no package or release writes | Unpublished candidate or exact verified-image evidence |
| Manual dispatch on the current green `main` revision | Reuse a compatible digest, or build, scan, run representative runtime qualification, push, attest, and cleanly verify it | `actions: read`, `contents: read`, `packages: write`, `attestations: write`, `id-token: write` | `build-<id>`, `verified-build-<id>`, and `sha-<git-sha>` all name the verified digest |
| Semantic `v<release_version>` tag | Qualify an existing digest; never rebuild | `actions: read`, `contents: write`, `packages: write`, `attestations: read` | Exact main-SHA and verified-build markers match before the immutable version alias and GitHub Release are created |

The image's OCI source label links the GHCR package to `eirhop/favn`.

The repository/package owner must perform the one-time GitHub package setup:

1. Confirm the repository permits GitHub Actions to create and write packages.
2. Keep the package private while Favn is private.
3. Confirm the created package inherits access from `eirhop/favn`.
4. Give deployment identities and developers pull-only package access.

Protect `main` with a repository ruleset that requires pull requests and the
two aggregate status checks `CI / CI` and
`Control-plane image / Control-plane image result`. Require the branch to be
up to date before merge and do not permit ordinary contributors to bypass
these checks. Add a separate `v*` tag ruleset that restricts tag creation to
release maintainers and prevents tag update or deletion. The manual image
workflow refuses a non-`main`, non-current, or non-green revision and checks
`main` again before recording immutable aliases. The promotion workflow
independently requires the tagged commit to be tested on `main` and all three
immutable registry markers to match, but repository rules prevent an operator
from bypassing that entry path accidentally.

Private pulls use a separately managed GitHub credential with `read:packages`
through the deployment platform or `docker login ghcr.io`. Favn application
configuration never accepts or stores a registry token.

## Deterministic build identity

`scripts/control_plane_build_id.exs` computes a canonical SHA-256 identity
without compiling the umbrella or downloading dependencies. The identity covers:

- source, `priv`, and asset inputs for `favn_core`, `favn_storage_postgres`,
  `favn_orchestrator`, and `favn_view`;
- production configuration and the complete `rel/control_plane` contract;
- the three repository-owned context selection, assembly, and canonical
  artifact-writer modules from `favn_local` (hashed as identity-only build
  inputs and never copied into the image);
- the non-optional transitive lockfile closure of the release's explicit
  production dependency roots;
- the pinned builder/runtime images, platform, Elixir, OTP, control-plane,
  manifest-schema, and runner-contract versions.

The descriptor's `control_plane_version` is the version compiled into the OTP
release and OCI label. It is not the repository-wide Favn release tag. Metadata
also emits the public `favn` package's `release_version`, but that value is not
part of image identity. Release promotion requires the Git tag to match it. A
runner-only Favn release can therefore add its new version alias to an older
compatible control-plane digest without changing that digest or its labels.

It deliberately excludes runner, authoring, local/development tooling, tests,
documentation, optional dependencies outside the release closure, and generated
Phoenix static output. Changing only an excluded input does not build another
official image. Missing dependency roots, unsafe paths, duplicate records, or
unreadable inputs fail closed.

For CI-readable metadata:

```bash
elixir scripts/control_plane_build_id.exs --metadata
```

## Qualification identities

Image construction, runtime compatibility, and vulnerability scanning have
separate deterministic identities. CI compares each identity between the base
and head revisions instead of using one broad changed-path boolean:

- `control_plane_build_id` covers the exact image bytes and build contract
  described above. Only a changed build ID creates a candidate image.
- `runtime_qualification_id` combines that build ID with runner, authoring,
  in-process DuckDB, SQL runtime, runner-build, Compose, container-acceptance,
  root test configuration, lockfile, registry, image-contract, and qualification
  workflow inputs. A changed runtime ID with an unchanged build ID pulls the
  existing verified image and runs compatibility acceptance without rebuilding
  or rescanning it.
- `security_scan_id` combines the build ID with the Grype policy, pinned scan
  behavior, image contract, and scheduled-scan workflow. A changed scan ID with
  an unchanged build ID rescans the existing verified image without running
  runner/Compose acceptance.

The runtime identity deliberately excludes ordinary application tests,
documentation, Azure and ADBC adapter code covered by their owning CI slices,
and local-only backfill, inspection, initialization, run, and run-list commands.
New or unclassified repository paths fail safe by selecting runtime
qualification. The focused routing contract is executable through:

```bash
elixir scripts/control_plane_qualification_test.exs
```

For CI-readable identities:

```bash
build_id=$(elixir scripts/control_plane_build_id.exs)
elixir scripts/control_plane_qualification_id.exs \
  --control-plane-build-id "$build_id" \
  --metadata
```

## Maintainer candidate build

From the Favn repository root:

```bash
MIX_ENV=prod mix favn.build.control_plane
MIX_ENV=prod mix favn.build.control_plane --load
```

The first command writes an immutable, integrity-checked, relocatable context at
`.favn/build/control-plane/<control_plane_build_id>/context`. The second also
uses Docker Buildx to load
`favn-control-plane-candidate:<control_plane_build_id>` for Linux amd64. This is
the only supported unpublished-image path. The task cannot select another image,
repository, platform, source root, or output root.

A consuming project can instead exercise a local Favn checkout through the
explicit development-only command:

```bash
FAVN_CHECKOUT=/absolute/path/to/favn mix favn.maintainer.dev
```

`FAVN_CHECKOUT` must be present before Mix evaluates dependencies. The task
proves that every loaded Favn path dependency belongs to that checkout, builds
or reuses its unpublished candidate, selects the exact local Docker image ID,
records both source and image identities, and starts the ordinary local stack.
It never publishes an image. Running `mix favn.install` afterwards deliberately
returns the project to the official image. The complete consumer setup is in
the [local development guide](../../apps/favn/guides/local-development.md#testing-a-local-favn-checkout).

Official images are built only by protected GitHub Actions. Pull requests build,
inspect, scan, and generate an SBOM for an unpublished candidate when the exact
image identity changed. Runtime-only changes qualify the existing verified
digest, and scan-only changes rescan that digest. If a pull request needs the
current build but it has not yet been published, its runtime or security job
builds an unpublished candidate instead of blocking on publication.

Merging to `main` never publishes an image. A maintainer manually dispatches the
workflow for the exact current `main` SHA after required CI succeeds. The
workflow builds or reuses that immutable build, always rescans it, reruns the
representative runtime qualification, and rechecks that `main` has not moved before
adding aliases. Only after both attestations and clean qualification succeed
does it add the immutable build, verified-build, and exact-SHA aliases. An
interruption before either attestation therefore leaves no official build cache
key and the next dispatch safely rebuilds. A build tag without its matching
verified-build marker is re-verified rather than trusted. Both the build marker
and exact-SHA marker are required by release promotion. Registry lookup and
validation errors fail the workflow; they are not interpreted as a cache miss.

Candidate and newly published images are scanned with pinned Grype `0.116.0`.
Any high or critical finding with an available fix fails the build. The tracked
policy at `security/control-plane-grype.yaml` names each Debian vulnerability,
package, and vendor fix state individually; it never hides all unfixed findings.
Those narrow exceptions stop matching as soon as Debian publishes a fix, and CI
also rejects image qualification and release promotion after the explicit
review deadline until maintainers upgrade the base snapshot or review each
remaining exception. Scan policy is not an image-byte input and therefore does
not change `control_plane_build_id`; policy-only pull requests instead pull and
rescan the exact verified digest. Manual publication always rescans the selected
`main` image. Release promotion also rescans, and the
trusted-default-branch `Control-plane security scan` workflow scans the current
verified digest daily so vulnerability-database freshness is not coupled to
unrelated pull requests.

## Runtime contract

The final image:

- is based on digest-pinned Debian builder and runtime images;
- downloads the fixed Tailwind and esbuild asset binaries before compilation
  and verifies their pinned SHA-256 checksums;
- records and verifies the actual Elixir `1.20.2` and OTP `29.0.3` files copied
  from the pinned builder;
- runs as non-root UID/GID `10001:10001` from `/app`;
- starts `/app/bin/favn_control_plane start`;
- exposes View port `4000` and private Orchestrator port `4101` without publishing
  either port itself;
- uses `/app/bin/favn_control_plane_health` for readiness-based container health;
- includes `/app/bin/favn_control_plane_ops` for the fixed release-safe database
  operation set;
- supports a read-only root filesystem with a writable, size-bounded `/tmp`.

The reference Compose deployment additionally drops every Linux capability and
sets `no-new-privileges:true` on the control plane, runner, and one-shot
control-plane operation containers. PostgreSQL remains reachable only on the
private application network. Deployments using another scheduler must preserve
those restrictions or document their platform-equivalent controls.

Phoenix generator templates and inert esbuild/tailwind configuration are removed
after release assembly. The final release contains no source templates, and its
runtime configuration and release metadata contain no dependency on builder
checkout paths. Stripped third-party BEAM/NIF debug or line-table data may retain
the container-local `/build` prefix; it is not a developer checkout path and is
not used at runtime.

Mix release assembly's generated fallback `releases/COOKIE` file is removed from
the final image. The launcher receives the distribution cookie only from the
required `FAVN_DISTRIBUTION_COOKIE` environment variable at boot.

The production environment and network requirements are defined in
[`control_plane_environment.md`](control_plane_environment.md). Database commands
and privilege separation are defined in
[`postgresql_operator_runbook.md`](postgresql_operator_runbook.md).
Artifact ownership, deployment order, and topology limits are defined in
[`deployment_topology.md`](deployment_topology.md).

`scripts/control_plane_image_contract.sh` is the canonical static/content
qualification. Release promotion additionally verifies the registry digest,
GitHub build-provenance attestation, and SPDX SBOM attestation before adding a
version alias.

## Production qualification

Maintainers can qualify an exact locally loaded candidate with:

```bash
FAVN_CONTROL_PLANE_CANDIDATE=favn-control-plane-candidate:<build-id> \
  mix test.container
```

This dedicated tier runs the final image with PostgreSQL 18 and a representative
customer-built runner. It proves that the generated customer Dockerfile builds,
the control plane and runner remain separate images and applications, the three
services become healthy, an aligned manifest activates, and stop/start reuses
the exact runner image and manifest identities. Product code never builds the
customer image.

Focused owning-app tests cover release operations, compatibility rejection,
bounded drain policy, conservative recovery, rotation parsing, and reload
rollback state. The representative container tier does not currently execute
SQL/Elixir smoke runs, real signal-loss drills, secret rotation, or
control-plane/runner upgrade and rollback. Those target-environment drills
remain required before a deployment claims production qualification. The exact
current evidence is listed in
[`issue_522_acceptance_matrix.md`](issue_522_acceptance_matrix.md).

Pull requests that change control-plane build inputs build and scan a candidate
and run this tier without registry write permission. Runner, in-process DuckDB,
runner-template, and Compose changes use the current verified digest when it exists;
when delayed manual publication means it does not, the job builds the same
unpublished candidate locally. Scan-policy changes rescan the exact verified
image or the equivalent unpublished candidate. Unrelated local commands, ADBC,
Azure, ordinary tests, and documentation run neither heavy image job.

An ordinary merge to `main` publishes nothing. Manual publication accepts only
the exact current green `main` revision, runs the full scan and container tier
even when its immutable build already exists, and checks `main` again before
updating aliases. Release promotion repeats qualification against the immutable
registry digest before adding the version alias. A failing required
qualification can therefore never publish or promote an untested image.
