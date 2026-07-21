# Issue 522 production release implementation plan

Status: temporary normative implementation plan for issue #522.

This document is the implementation guide and review checklist for issue #522.
It records the complete first production-release contract. If implementation
work discovers that this contract cannot be followed, update this document and
issue #522 before merging a different design.

## 1. Assumptions and locked decisions

The implementation is based on these assumptions:

1. The first deployment is an internal business application operated by a
   trusted team on an operator-managed container platform.
2. One control-plane node and one runner node are sufficient for the first
   supported production release.
3. Customer Elixir code is normal, so the customer builds the final runner
   image. Favn publishes the control-plane image.
4. PostgreSQL is the only production storage implementation.
5. Production secrets are supplied as environment variables, read at boot, and
   rotated by an operator through a controlled restart.
6. A manifest may be deployed without rebuilding the runner only when its
   required runtime code has the exact canonical fingerprint of the
   already-built runner release.
7. Production upgrades are scheduled, drain-first operations. Zero-downtime
   deployment is not part of the first operational contract.
8. Docker Engine with Docker Compose v2 is a mandatory local-development
   prerequisite. The supported local runtime uses containers instead of
   compiling and launching the control plane from source.
9. The supported development hosts are Linux amd64 and amd64 WSL2 using Docker
   Engine or Docker Desktop with Compose v2. Native Windows, macOS/arm64,
   linux/arm64, and Podman compatibility are not claimed by this release.
10. Until Favn packages are published, a customer project consumes a pinned
    Favn repository checkout through path dependencies. The runner build vendors
    its exact dependency closure so its OCI build requires neither that checkout
    nor GitHub credentials.
11. Unpublished control-plane images are supported only by a repository-owned
    maintainer/acceptance path. Public mix favn.install resolves official GHCR
    releases and exposes no arbitrary-image or source-build fallback.

These decisions are normative:

- Favn View and Favn Orchestrator run in one control-plane BEAM and one
  control-plane container.
- Favn Runner runs in a second BEAM and second container.
- The control plane and runner communicate over distributed Erlang on a private
  network.
- The control plane has one statically configured runner node.
- PostgreSQL 18 is external to both BEAMs.
- Only OCI container images are production runtime artifacts.
- A manifest release remains an independent, immutable publication artifact.
- The canonical Favn control-plane image is published to
  ghcr.io/eirhop/favn-control-plane. It is private until Favn is intentionally
  made public.
- The operator chooses and configures the registry for the customer-built
  runner image. An operator may mirror the control-plane digest into that
  registry, but mirroring is optional and is not performed by Favn tooling.
- Favn standardizes the portable deployment contract and security requirements,
  not one cloud provider's networking or infrastructure resources.
- Every production-dependent setting is read from environment variables at
  runtime. Build-time configuration must not vary a production image.
- No production supervisor starts and no HTTP listener opens until runtime
  configuration has been validated.
- mix favn.install pulls the version-matched prebuilt control-plane image and
  never builds the control plane.
- mix favn.dev runs PostgreSQL, the prebuilt control plane, and the
  customer-built runner as containers on one private Docker Compose network.
- Favn distributes the Elixir DSL, authoring/compiler contracts, public Mix
  tasks, runner build inputs, and shared runner/control-plane contracts needed
  by a customer project. It does not distribute the complete control-plane
  source tree as a supported local runtime or retain a source-built fallback.

## 2. Completion definition

Issue #522 is complete only when a clean environment can:

1. Run release-safe PostgreSQL migration and provisioning commands from the
   published control-plane image.
2. Start the published control-plane image without the repository, Mix, source
   files, or build tools.
3. Build a customer runner image from a normal Favn project.
4. Start that runner image as a separate named BEAM node.
5. Publish a manifest release and its missing content-addressed execution
   packages.
6. Prove the runner advertises the exact runner release required by the
   manifest.
7. Activate the manifest only after that proof succeeds.
8. Execute one SQL asset and one Elixir asset through the two-container
   topology.
9. Preserve control-plane state across control-plane and runner restarts.
10. Permit a SQL-only manifest update without rebuilding the runner.
11. Reject a manifest-only update after relevant Elixir code, runtime resolver
    code, plugin code, or runtime dependencies change.
12. Drain or explicitly cancel active work during a bounded SIGTERM shutdown.
13. Rotate environment-supplied service credentials and runtime-input keys
    through the documented manual restart procedure.
14. Perform one compatible control-plane upgrade and rollback drill.
15. Install and run the local Docker Compose topology without compiling Favn
    View, Orchestrator, or PostgreSQL storage in the customer project.
16. Prove local SQL/manifest-only reload does not rebuild either runtime image
    and local Elixir/runtime changes rebuild only the runner image.
17. Prove an unrelated runner, authoring, documentation, test, or development
    tooling change does not build a new official control-plane image.
18. Pass the focused, acceptance, slow, security, image-content, and
    documentation checks defined in this plan.

No existing metadata-only build output counts as a runnable production artifact.

## 3. Supported topology

### 3.1 Control-plane container

The control-plane release is named favn_control_plane. It includes:

- favn_view;
- favn_orchestrator;
- favn_storage_postgres;
- favn_core and required runtime libraries;
- the scheduler and all control-plane background workers;
- the private orchestrator publication/API listener;
- the Phoenix/LiveView operator listener;
- static web assets and their digest.

It must not include:

- favn_runner as a runtime application;
- customer asset modules;
- customer plugins;
- Mix or development tools;
- a local storage implementation.

Favn View must call backend behavior only through the public FavnOrchestrator
facade in the same BEAM. It must not call storage, Repo, scheduler, runner,
persistence, compiler, or plugin internals. There is no internal HTTP hop from
View to Orchestrator.

The private orchestrator HTTP listener remains available for manifest
publication and machine operations. It binds to the private interface and is
authenticated with service tokens. The public reverse proxy normally exposes
only the Favn View listener.

### 3.2 Runner container

The customer runner release is named favn_runner. It includes:

- favn_runner;
- favn_core and favn_sql_runtime;
- the selected runner plugins and adapters;
- the customer's compiled application and runtime dependencies;
- the immutable runner release descriptor;
- only the operating-system libraries needed by the release and selected
  adapters.

The runner does not include Favn View, Favn Orchestrator, PostgreSQL control
plane storage, or operator endpoints.

The runner is trusted code in the same operational trust zone as the control
plane. A distributed Erlang cookie grants node-level trust and is not an
application-scoped permission.

### 3.3 PostgreSQL

PostgreSQL 18 is the sole durable authority. It is reached only from the
control-plane private network. Runtime processes use the restricted runtime
role. Migrations and grants use a separately authorized release-task identity.

The application never creates or migrates the schema during normal startup.
Startup validates connectivity, the exact schema version, required objects, and
runtime grants, then fails readiness if any check is not satisfied.

### 3.4 Network boundary

The supported deployment has:

- a dedicated, trusted network segment configured by the operator;
- PostgreSQL reachable only from allowed private subnets;
- the control-plane and runner EPMD and distribution ports reachable only
  between those two trusted containers;
- no BEAM distribution, EPMD, PostgreSQL, or private orchestrator API port
  exposed to the internet;
- operator access through a VPN or an authenticated TLS reverse proxy;
- TLS termination at the reverse proxy for browser traffic;
- TLS with full certificate verification for PostgreSQL.

The first runner transport uses plain distributed Erlang over that trusted
network plus a strong random distribution cookie. It does not configure TLS
distribution. The documentation must state that traffic is unencrypted, the
cookie handshake is not a cryptographic transport boundary, and a connected
runner has node-level trust. Container, firewall, and network rules are hard
requirements of the supported security contract, not optional recommendations.
A non-public IP address alone is insufficient. Support for less isolated
deployments is tracked in issue #530.

### 3.5 Local Docker Compose topology

Docker Engine with Docker Compose v2 is the only supported local runtime for
this release. Podman compatibility, a native control-plane OTP archive, and a
source-built control-plane fallback are not alternate supported modes.

The host support matrix is deliberately narrow: native Linux amd64 and amd64
WSL2 with a reachable Linux-container Docker daemon. mix favn.doctor rejects an
unsupported host architecture or container target with a stable explanation.
Docker emulation on Apple Silicon, native Windows containers, and alternate OCI
engines are not part of the acceptance matrix.

mix favn.dev generates and owns one project-scoped Compose application with:

- one digest-pinned PostgreSQL 18 container and persistent named volume;
- one control-plane container using the exact GHCR digest resolved by
  mix favn.install;
- one customer runner container built locally from the same generated runner
  context used for production;
- one project-scoped private bridge network with stable service DNS names.

The runner and control plane use long distributed Erlang node names derived
from stable Compose service names. EPMD and fixed distribution ports are
reachable only on the Compose network and are never published to the host. The
View and private orchestrator HTTP ports bind only to 127.0.0.1 so the local
browser and Mix tasks can reach them. PostgreSQL is not published unless a
documented diagnostic command explicitly requires temporary loopback access.

The local stack uses the production control-plane image without bind-mounting
the repository, source, Mix, or build tools into it. The local runner image is
also an actual release image rather than a host Mix process. Docker layer cache
may accelerate runner rebuilds, but a running release never reads customer
source from a bind mount.

Local-only environment differences are explicit: generated development
secrets, loopback HTTP, PostgreSQL plaintext on the private local bridge, local
workspace provisioning, and scheduler-disabled-by-default behavior. These are
runtime values accepted only by the documented local-development mode; the
image and application closure are otherwise the production control plane.

## 4. Artifact contract

### 4.1 Published control-plane image

Favn CI builds one Linux amd64 OCI image from the Favn repository. The image is
published at ghcr.io/eirhop/favn-control-plane and versioned by an immutable
registry digest, a deterministic control-plane build tag, a source Git SHA tag,
and an immutable Favn release tag. It contains no customer code and is suitable
for every compatible installation.

The first GitHub Actions push creates the GHCR package as private and links it
to https://github.com/eirhop/favn. The image has the
org.opencontainers.image.source label set to that repository. The publishing
job uses its repository GITHUB_TOKEN with only these elevated job permissions:

    contents: read
    packages: write
    attestations: write
    id-token: write

No long-lived registry publishing token is stored in repository secrets. The
package inherits access from the source repository. Private deployments and
developers authenticate pulls with a separately issued read:packages token;
the token is configured in the deployment platform or docker login and is
never accepted or stored by Favn application code.

The repository command mix favn.build.control_plane creates or verifies the
control-plane release and Docker build context. It is a maintainer-oriented
command and must produce an operational artifact, not metadata claiming that a
future artifact could be built.

The CI output records:

- image repository and immutable digest;
- control-plane OTP application version for a newly built digest, kept distinct
  from the repository-wide Favn release version alias;
- Git commit SHA;
- Elixir and OTP versions;
- manifest schema version;
- runner contract version;
- base image digest;
- target OS and architecture;
- static asset digest;
- build timestamp as non-identity metadata.

#### 4.1.1 Selective build identity

The maintainer build produces control_plane_build_id, a lowercase SHA-256 digest
over canonical records for every input that can change final image bytes or the
control-plane runtime contract:

- production lib and priv files for favn_core, favn_orchestrator,
  favn_storage_postgres, and favn_view;
- the explicit favn_control_plane release application closure and relevant
  application mix.exs files;
- the normalized production dependency lock entries reachable from that
  closure, including dependency versions and source checksums;
- environment-independent production config, release overlays, runtime config
  loader inputs, migrations, and compiled Phoenix asset inputs;
- the control-plane Dockerfile, ignore rules, release assembly code, and base
  image references;
- exact Elixir, OTP, target OS, and linux/amd64 target identifiers;
- manifest schema and runner contract versions embedded in the control plane.

Canonical records are sorted by normalized repository-relative path and hash.
Timestamps, checkout paths, branch names, source Git revision, test files, and
documentation are excluded from identity. The Git revision is recorded as
non-identity provenance only when new image bytes are built. The build task
emits both the input record set and its ID for review without including source
content or secrets.

The control-plane OTP application version is part of image identity because it
is compiled into the release. The repository-wide Favn release version is the
semantic Git/GHCR alias and is not part of image identity. A runner-only or
development-tooling-only release may therefore add a new Favn version alias to
the existing compatible control-plane digest.

The control-plane production closure must not include favn_runner, favn_local,
favn_authoring, favn, test-support applications, or development-only
dependencies. Therefore ordinary edits confined to runner code, customer
authoring code, public development tooling, tests, examples, or documentation
do not change control_plane_build_id and do not build a new official image.
Explicit root-owned control-plane release assembly files remain relevant even
though they are maintainer tooling.

A root mix.lock edit is not automatically an image change. The input collector
normalizes only lock entries reachable from the production control-plane
closure. A runner-only dependency update therefore leaves
control_plane_build_id unchanged. Failure to resolve the dependency closure is
a hard CI failure, never permission to reuse an older image.

#### 4.1.2 CI build and promotion algorithm

The control-plane workflow has a cheap input-discovery job and an expensive
image job:

1. Pull requests compute the base and head control_plane_build_id values. If
   they are equal, candidate image jobs are skipped. If they differ, CI builds
   an unpublished linux/amd64 candidate, starts and inspects it, runs the PR
   container acceptance described below, and scans it. Pull-request jobs have
   contents:read and no packages:write permission; they cannot publish to GHCR.
2. The main-branch workflow runs from the exact merged commit only after the
   ordinary main test aggregate succeeds. It computes the candidate ID and
   queries GHCR for build-<control_plane_build_id>.
3. If that immutable build tag exists, main verifies its digest, provenance,
   OCI labels, contract versions, and target. It reuses the digest and performs
   no Docker build or control-plane image acceptance.
4. If the tag does not exist, main rebuilds the exact merged candidate, runs the
   pre-publish checks, and pushes only a run-scoped staging tag. It generates
   provenance and an SBOM for that digest, then pulls and inspects the digest in
   a clean verification job. An interruption before or between attestations
   leaves no official build cache key, so a retry safely rebuilds.
5. Only after attestation and clean verification succeed does CI add immutable
   build-<control_plane_build_id>, verified-build-<control_plane_build_id>, and
   sha-<git_sha> aliases for the digest. A failure adds none of them. If CI is
   interrupted after writing the build alias but before both verification
   markers, the next run verifies the existing digest and finishes the markers;
   it never treats the unmarked tag as trusted. Release promotion requires the
   build, verified-build, and exact tagged-main SHA references to resolve to the
   same digest.
6. A Favn release adds v<version> as another immutable alias of the verified
   digest. Runner-only or development-tooling-only releases therefore receive
   a version tag that reuses the previous control-plane digest without building
   new image bytes.
7. CI refuses to overwrite build, SHA, or Favn version tags when an existing
   tag points to a different digest.
8. Production documentation and generated release metadata always use the
   repository@sha256:<digest> reference. Tags are lookup and human-navigation
   aids, not deployment identity.

The release workflow generates build provenance and an SBOM for newly built
digests. Reusing a digest reuses its existing provenance and SBOM instead of
claiming a new build occurred.

#### 4.1.3 CI event and test matrix

Every code-affecting pull request runs the existing format, warnings-as-errors
compile, test-tag guard, fast umbrella, acceptance, and slow slices required by
the affected applications. Documentation-only changes do not run runtime or
image jobs. Branch protection requires the aggregate PR result before merge.

When a pull request changes control_plane_build_id, its unpublished candidate
additionally runs:

- release assembly and final-image content inspection;
- PostgreSQL 18 migrate, grant, provision, schema, and permission verification
  through release-safe commands;
- the golden private-network control-plane plus canonical-runner scenario,
  including readiness, one SQL asset, one Elixir asset, and a manifest-only
  update;
- proof that favn_runner, Mix, source, build caches, and unexpected applications
  are absent from the control plane;
- non-root/read-only-filesystem, private-port, redaction, vulnerability, idle
  SIGTERM, and bounded shutdown smoke checks.

When a pull request changes runner inputs but not the control plane, it skips the
control-plane image build and runs the customer runner build/self-verification,
manifest-alignment, mismatch, and runner container acceptance. When it changes
favn_local/public development tooling, it runs the local Compose acceptance
against the current official digest, or against the repository candidate when
the same pull request also changes the control plane.

The main push reruns the ordinary test aggregate for the exact merged commit.
For a new control-plane ID, main rebuilds the image, then runs the complete
pre-publish production container acceptance: release-safe database operations,
startup/readiness, SQL and Elixir execution, restart/persistence, manifest-only
update, forged mismatch rejection, active and idle shutdown, content/security
checks, and local Compose compatibility. Only then may it push the immutable
build/SHA tags. For an unchanged ID, main skips all control-plane build and
image-acceptance work while still running runner or development-tooling tests
selected by those changes.

Creating a Favn release is a separate promotion event. It pulls the already
verified digest, runs upgrade/rollback and manual-rotation qualification against
that digest, verifies provenance/SBOM, and only then creates the immutable Favn
version alias and release notes. A main-branch image is therefore a verified
candidate; a version-tagged image is a release-qualified artifact.

### 4.2 Customer-built runner image

The public command mix favn.build.runner builds the customer project and writes
an immutable runner build directory:

    .favn/dist/runner/<runner_release_id>/
      Dockerfile
      runner-release.json
      bundle.json
      manifest/
        manifest-index.json
        execution-packages/<content_hash>.json
        bundle.json
      release-input/
      operator-notes.md

The exact location may continue to use the existing Favn.Dev.Paths helpers, but
the directory name and every reference inside it must use runner_release_id,
not a timestamp-only build ID.

The generated Dockerfile uses pinned official Elixir/Erlang builder and
operating-system runtime images. The user or the user's CI invokes a normal OCI
build command against that context and pushes the resulting image. Favn does not
publish a runner image or a Favn-specific runner builder/base image, does not
own registry credentials, and does not push the customer artifact.

The generated context must be relocatable. It may contain paths relative to the
context, but build.json, bundle.json, image labels, release descriptors, and
operator notes must not contain the source checkout's absolute path.

The generated Dockerfile must:

- pin builder and runtime base images by digest;
- build with MIX_ENV=prod;
- compile the customer project and selected runtime dependencies;
- assemble the favn_runner release;
- copy only the release, descriptor, and required OS runtime files to the final
  image;
- run as a non-root user;
- use a read-only root filesystem contract, except for an explicitly documented
  temporary directory;
- expose only EPMD and the configured fixed distribution port;
- use the release binary as its entrypoint;
- add OCI labels for the runner release ID, Favn version, Git SHA when available,
  runner contract version, target, and build time.

The runner build task must not claim success merely because it copied BEAM
files. Its acceptance test builds the generated container and starts the
release.

### 4.3 Manifest release

A manifest release is independent of the runner image. Its directory contains:

- one canonical manifest index;
- every execution-package content hash referenced by that manifest;
- only the missing content-addressed execution-package payloads needed for
  publication;
- the required runner release ID;
- schema and runner contract versions;
- a bundle descriptor containing hashes and sizes for every file.

The command mix favn.build.manifest requires an explicit runner descriptor:

    mix favn.build.manifest \
      --runner-release .favn/dist/runner/<id>/runner-release.json

The task compiles the current project, regenerates the manifest and execution
packages, recomputes the current runner fingerprint, and compares it with the
supplied descriptor.

- An exact fingerprint match produces a manifest release that reuses the
  descriptor's runner_release_id.
- A mismatch fails with a stable runner_rebuild_required error and prints the
  changed fingerprint categories without exposing source or secret values.
- A missing, malformed, future-version, or incompatible descriptor fails before
  writing a publishable bundle.

mix favn.build.runner invokes this same manifest builder after generating the
new runner descriptor. There is one implementation of manifest bundle creation
and one implementation of fingerprint comparison.

### 4.4 Publication and activation commands

Replace mix favn.bootstrap.single with two topology-neutral operations:

- mix favn.publish publishes missing execution packages and then publishes the
  immutable manifest version as staged/inactive.
- mix favn.activate activates one exact staged manifest version for one
  workspace after the control plane proves the configured runner advertises the
  required release.

Both commands accept the private orchestrator URL through an option or
FAVN_ORCHESTRATOR_URL. Authentication is read from
FAVN_ORCHESTRATOR_SERVICE_TOKEN; it must never be accepted in a command-line
argument, written into a bundle, or printed.

Publication is content-addressed and idempotent. Repeating it returns
already_published for matching content and fails on identity/content conflicts.
Activation is idempotent for the exact active deployment and returns an
explicit mismatch if the connected runner is absent, unhealthy, incompatible,
or advertises a different runner release.

### 4.5 Local installation and development artifact contract

Before Hex/package publication exists, the documented consumer setup pins one
Favn Git tag or commit in a separately checked-out repository and uses path
dependencies rooted in that checkout. CI checks out that exact revision as an
explicit second source. Floating branches are unsupported. The checkout may
physically contain the monorepo, but Mix compiles only the public DSL,
authoring, local-tooling, runner, SQL-runtime, shared-contract, and selected
plugin/adapter dependency closure; it does not compile or launch favn_view,
favn_orchestrator, or favn_storage_postgres for local use.

mix favn.build.runner copies the exact required Favn/customer/plugin sources or
precompiled release inputs plus normalized lock data into the relocatable
runner context. The generated Docker build performs no git clone, does not read
the sibling Favn checkout, and requires no GitHub, Hex-private-repository, or
registry credential other than credentials the operator separately needs to
push the completed runner image.

Each Favn release has an immutable v<version> GHCR tag. mix favn.install derives
that tag from the installed Favn tooling version, pulls it, resolves the
registry-reported RepoDigest, validates the OCI labels and supported contract
versions, and records the exact repository@sha256:<digest> reference in
.favn/install/control-plane.json. Compose always consumes the recorded digest,
never the version tag.

If a Favn release contains only runner or development-tooling changes, its
version tag points to the previously built compatible control-plane digest. The
image's source-revision label truthfully remains the revision from which those
image bytes were built; the Favn release tag is only a compatibility alias.

mix favn.install is explicit and does not start services. It must:

- verify Docker Engine is reachable and Docker Compose v2 is available;
- fail with targeted installation guidance when either prerequisite is absent;
- pull the private image using existing Docker credential-helper/docker login
  configuration without accepting a registry password or token argument;
- support offline reuse only when the exact digest is already present locally
  and passes inspection;
- install or verify only the Elixir dependencies and build inputs needed for
  authoring, manifest generation, public local tooling, and the customer
  runner;
- generate project-scoped Compose metadata and install state atomically;
- remove the current full-umbrella runtime materialization, control-plane Mix
  dependency installation, and Phoenix asset installation behavior;
- remove public skip_web_install and skip_runtime_deps_install options because
  the work they skip no longer exists;
- remove the public skip_tool_checks option because Docker and Compose checks
  are mandatory; tests use injected command/inspection boundaries instead;
- retain --force as an explicit revalidation/repull/regeneration path;
- never fall back to compiling or starting a local control-plane source tree.

Repository maintainers use mix favn.build.control_plane --load to assemble,
build, and load an unpublished local image tagged
favn-control-plane-candidate:<control_plane_build_id>. The task returns the
Docker image ID and the same input descriptor used by CI. Repository acceptance
helpers may inject that exact image ID through an internal typed option while
running tests. The public mix favn.install argument parser does not expose the
injection, does not accept a mutable candidate tag or arbitrary image, and does
not write candidate state as an official GHCR installation. Candidate images
are never pushed by pull-request workflows.

mix favn.dev validates install state and then performs this ordered workflow:

1. Recompute the customer runtime fingerprint and build the initial runner
   release/image if the exact runner_release_id is not already available.
2. Start the digest-pinned PostgreSQL service and wait for its health check.
3. Run migration, grant, verification, and local workspace provisioning as
   one-shot commands from the installed control-plane image.
4. Start the runner container and verify its baked release descriptor.
5. Start the control-plane container configured for that runner node and wait
   for liveness and full readiness.
6. Publish and activate the aligned local manifest.
7. Stream prefixed Compose logs in the foreground and print the loopback UI and
   private API URLs.

Ctrl-C performs bounded graceful Compose stop in runner/control-plane order and
preserves PostgreSQL data. A failed partial startup stops containers created by
that attempt, preserves logs and the database volume, records the failing
phase, and never reports the stack ready.

mix favn.reload uses the same canonical change classification as production:

- A manifest/SQL-only change rebuilds and publishes only the manifest bundle;
  neither container image is rebuilt or restarted.
- A customer Elixir, helper, resolver, plugin, adapter, runtime dependency, or
  runner contract change builds a new runner release/image, asks the control
  plane to drain local work, replaces only the runner service, verifies the new
  descriptor, and then publishes and activates the aligned manifest.
- If drain cannot complete within the documented local timeout, reload fails
  without replacing the runner or activating the manifest. It does not silently
  cancel active work.
- mix favn.reload never builds or replaces the official control-plane image.

The remaining local commands operate through Compose and public runtime
boundaries:

- mix favn.status combines docker compose ps with bounded control-plane and
  runner health/release diagnostics;
- mix favn.logs reads bounded/prefixed Compose service logs;
- mix favn.run and operational commands call the loopback private API;
- mix favn.stop gracefully stops the project Compose application and preserves
  the database volume and cached images;
- mix favn.reset stops/removes project containers, removes the project
  PostgreSQL volume, generated local runner images, manifests, and local state
  only with an explicit --yes destructive confirmation; without --yes it lists
  the exact project-scoped resources and refuses deletion;
- mix favn.doctor and mix favn.diagnostics report Docker/Compose availability,
  GHCR image/digest/label state, Compose network isolation, service health,
  runner release alignment, and PostgreSQL readiness without exposing secrets.

## 5. Runner release identity

### 5.1 Why two version fields are required

runner_contract_version describes the protocol and serialized structures
understood by Favn versions. It does not identify customer code.

runner_release_id identifies the exact customer runtime code and runtime
dependency fingerprint that a manifest requires. Both fields are required.

The OCI digest is deliberately not part of runner_release_id. Including the
final image digest would create a circular build dependency. Operators deploy
and audit the OCI digest separately; the running node reports the baked
runner_release_id.

### 5.2 Canonical runner descriptor

Add a public, serializable Favn.RunnerRelease descriptor in favn_core. Its
canonical identity payload has:

| Field | Meaning |
| --- | --- |
| schema_version | Descriptor schema, initially 1 |
| favn_version | Favn release used to build the runner |
| runner_contract_version | Runner protocol version |
| elixir_version | Exact Elixir build version |
| otp_release | Exact OTP major/release target |
| target | Normalized linux/amd64 target identifier |
| runtime_code_digest | Digest of the canonical runtime BEAM set |
| runtime_dependency_digest | Digest of the normalized runtime dependency/lock input |
| runtime_modules | Sorted module names and individual canonical BEAM digests |
| runtime_applications | Sorted application names, versions, and lock fingerprints |
| plugins | Sorted plugin/adapter names, versions, modules, and declared capabilities |
| build_profile | Fixed prod release profile identifier |

runner_release_id is calculated as:

    "rr_" <> lowercase_hex(sha256(canonical_json(identity_payload)))

The identity payload excludes:

- runner_release_id itself;
- build time;
- checkout and build paths;
- image tag and image digest;
- Git branch;
- manifest version ID and manifest content hash;
- SQL execution-package content;
- pipeline and schedule authoring metadata.

The serialized runner-release.json includes the identity payload,
runner_release_id, and non-identity build metadata. Decoding must reject unknown
required schema versions, malformed digests, duplicate modules/plugins, an ID
that does not match the canonical payload, and unsupported Favn/runner contract
combinations.

Use Favn.Manifest.Serializer or an equally deterministic core serializer.
Identity generation, validation, and hashing belong in favn_core. OCI and
filesystem assembly stay in favn_local.

### 5.3 Runtime module fingerprint

The runtime root modules are:

1. Every manifest asset with type :elixir.
2. Every runtime-input resolver referenced by a SQL execution package.
3. Every configured runner plugin, adapter, or customer supervised child.
4. Every module explicitly listed in the customer runner build configuration
   for dynamic dispatch.

Pipeline modules, schedule modules, pure SQL asset modules, and source assets are
not roots unless the same module also owns an Elixir asset or is reached from a
runtime root.

From those roots, the build performs a transitive closure over imports in the
compiled BEAM files. Project-local modules and project-local protocol
implementations in that closure are added. Third-party applications are
represented by the runtime dependency fingerprint.

Dynamic calls cannot always be discovered from BEAM imports. Add a documented
customer build setting:

    config :favn,
      runner_build: [
        extra_modules: [MyApp.DynamicHelper],
        extra_applications: [:my_runtime_app]
      ]

This is build input, not production deployment configuration. Values must be
module/application atoms known at build time. The build fails if an explicit
module cannot be loaded or its BEAM cannot be read.

For each selected module, hash a canonical executable BEAM representation with
debug information, docs, compile timestamps, and absolute source paths removed.
Do not hash the unmodified BEAM file. Add a regression test that compiles the
same source in two absolute checkout directories and obtains the same module
digest.

The canonical representation uses only executable chunks retained by the
default stripped OTP release so build-time and runner self-verification bytes
are identical. Compiler source provenance is unavailable after stripping, so a
selected module containing any absolute path in its executable literal table is
rejected with a stable, redacted build error instead of guessing whether the
literal came from `__DIR__`, `__ENV__.file`, or business code. Deployment paths
must be supplied through runtime configuration. Strip-removable attributes,
local symbol tables, line tables, compiler info, docs, and debug chunks never
participate in identity.

Protocol dispatch is dynamic. The closure therefore conservatively includes
every project-local protocol implementation in the supplied module set,
including implementations of protocols owned by dependencies, even when a
static import path cannot prove that one implementation will be called.

The runtime dependency digest covers:

- the normalized lock entries for runtime dependencies;
- application names and versions in the assembled release;
- selected plugin/adapter versions;
- native library or adapter fingerprints already exposed by those plugins.

Changing mix.lock is conservatively treated as a runner change. A customer may
avoid unnecessary image rebuilds by keeping authoring-only dependencies
separate from runtime dependencies, but Favn must prefer a false-positive
rebuild over accepting unverified runtime code.

If a module contains both SQL and Elixir assets, any source change that changes
that module's canonical BEAM digest requires a runner rebuild. Documentation
should recommend separate modules when SQL-only deployment independence is
important.

### 5.4 Change classification

The build system must produce these results:

| Change | New manifest | New runner release |
| --- | --- | --- |
| SQL text or SQL template | Yes | No |
| SQL checks, contracts, or materialization metadata | Yes | No |
| Asset dependencies, pipeline membership, or schedule metadata | Yes | No |
| Settings compiled into the manifest | Yes | No |
| Elixir asset implementation | Yes | Yes |
| Helper imported by an Elixir asset | Yes | Yes |
| Runtime-input resolver implementation | Yes | Yes |
| Explicit extra runtime module | Yes | Yes |
| Runner plugin, adapter, or supervised child | Yes | Yes |
| Runtime dependency lock/version | Yes | Yes |
| Favn runner contract version | Yes | Yes |
| Control-plane-only patch with compatible manifest and runner contracts | No | No |
| Runner base image OS patch with unchanged baked runtime descriptor | No | Image digest only |

The manifest is regenerated whenever a runner is rebuilt so the two artifacts
are aligned. A runner-only image is not considered deployable until its aligned
manifest bundle has also been produced.

### 5.5 Runtime self-verification

Embed runner-release.json inside the runner release at a fixed private path.
FavnRunner.ReleaseVerifier reads it before the runner server starts, validates
the descriptor, recomputes the runtime module/application fingerprint from the
loaded release, and compares it with the descriptor.

The runner must fail startup on:

- a missing descriptor;
- an invalid descriptor self-hash;
- a missing required module/application;
- a code or dependency fingerprint mismatch;
- an incompatible Favn or runner contract version.

Expose FavnRunner.release_info/0 through the public runner facade. It returns
only the validated descriptor's operational identity fields. Diagnostics and
readiness include runner_release_id, Favn version, runner contract version,
node name, and health status; they do not include paths, cookies, environment
values, source metadata, or runtime-input values.

## 6. Manifest and execution contracts

### 6.1 Core manifest changes

Add required_runner_release_id to:

- Favn.Manifest;
- Favn.Manifest.Version;
- the canonical serializer and rehydrator;
- manifest identity hashing;
- manifest compatibility validation;
- publication DTOs and API validation;
- public types, moduledocs, guides, and AI breadcrumbs.

The field is a required string matching the exact rr_<64 lowercase hex>
identity. It is part of the manifest content hash. It may not be supplied
independently from the runner descriptor during a local build.

Bump the manifest schema version once for this new required field. Bump the
runner contract version once for the new release identity carried through
runner work, results, diagnostics, and inspection requests. Update all fixtures
through shared favn_test_support helpers instead of scattering literal IDs.

Favn.Manifest.Version.from_published/2 must accept the envelope's
required_runner_release_id and prove it matches the canonical manifest field,
just as it already verifies schema version, runner contract version, and
content hash.

### 6.2 Persistence

Add required_runner_release_id to the PostgreSQL manifest_versions schema,
record codec, insert/read queries, operator reads, and diagnostics.

The database column may remain null only for historical manifests with a schema
version older than the new manifest schema. New-schema rows require a valid
runner release ID through a database CHECK constraint. Old manifests remain
readable for audit but cannot be newly activated.

Do not duplicate the field in workspace_deployments. A deployment already
references one immutable manifest version, which is the authority for the
required runner release. Active-deployment DTOs and operator views derive and
display the ID through that relationship.

Before upgrading an existing database, a release preflight must report active
deployments using an old manifest schema. The operator republishes and
activates aligned current manifests during the scheduled maintenance window.

### 6.3 Run pinning

Add required_runner_release_id to:

- FavnOrchestrator.RunState;
- the durable run snapshot codec;
- Favn.Contracts.RunnerWork;
- Favn.Contracts.RunnerResult and runner events where identity is echoed;
- relation-inspection requests that can execute runner code;
- execution lifecycle telemetry and redacted operator diagnostics.

The submission builder obtains the ID only from the immutable deployment
manifest. A caller cannot override it. The run snapshot pins manifest version
ID, manifest content hash, deployment ID, and runner release ID together.

Increment the run snapshot format. Historical terminal snapshots may decode a
missing runner release ID as nil for read-only display. Recovery or dispatch of
non-terminal work without a valid ID must stop with an explicit
legacy_runner_release_unbound error.

Every runner operation that can load a manifest or execute customer code checks
that the work's required_runner_release_id equals the runner's baked
runner_release_id. The runner returns a stable, non-retryable
runner_release_mismatch error before acquiring or starting work.

The orchestrator performs the same check before dispatch. The runner-side check
is still mandatory so stale or incorrectly constructed work cannot bypass the
contract.

### 6.4 Publication, staging, and activation

Publishing a manifest:

1. authenticates the service token;
2. validates compressed and decompressed limits;
3. validates every content hash and execution-package reference;
4. validates schema and runner contract compatibility;
5. validates required_runner_release_id syntax and internal consistency;
6. stores missing packages;
7. stores the immutable manifest;
8. leaves it inactive.

Publication does not require the runner to be online.

Activating a manifest:

1. loads the exact staged manifest from PostgreSQL;
2. rejects an old manifest schema or unsupported runner contract;
3. calls the configured runner client diagnostics/release-info path with a
   bounded timeout;
4. requires a connected, ready runner;
5. requires exact runner_release_id equality;
6. registers or verifies the manifest in the runner cache;
7. commits the immutable workspace deployment and active pointer;
8. emits an audit entry and telemetry with IDs, never secret material.

If multiple configured workspaces exist, every active workspace manifest must
be executable by the single current runner release. During a code upgrade the
operator stops admission, deploys the new runner, and activates each aligned
workspace manifest. Readiness remains false and old work is rejected until all
active workspace manifests are aligned.

SQL-only activation follows the same checks and succeeds because the new
manifest retains the existing required_runner_release_id.

## 7. Runner transport

### 7.1 Client implementation

Rename FavnOrchestrator.RunnerClient.LocalNode to
FavnOrchestrator.RunnerClient.BeamNode and remove the local in-process
production fallback. The implementation continues to satisfy
Favn.Contracts.RunnerClient and uses Node.connect plus :erpc with bounded
timeouts.

Production config always supplies:

- runner_node;
- runner_module, fixed to FavnRunner;
- dispatch timeout;
- await-result timeout buffer;
- diagnostics timeout.

Tests may inject a fake runner client. Local developer tooling may configure a
separate local runner node, but production code must never load favn_runner in
the control-plane BEAM.

Delete control-plane readiness logic that checks Code.ensure_loaded(FavnRunner)
locally. All runner availability and release information is obtained through
the configured RunnerClient diagnostics callback.

### 7.2 Node configuration

Both container releases use long node names. Required runtime values are:

- FAVN_CONTROL_PLANE_NODE for the control-plane RELEASE_NODE;
- FAVN_RUNNER_NODE for the runner RELEASE_NODE and control-plane target;
- FAVN_DISTRIBUTION_COOKIE for both RELEASE_COOKIE values;
- FAVN_BEAM_DISTRIBUTION_PORT for the fixed inet_dist_listen_min and
  inet_dist_listen_max value;
- ERL_EPMD_PORT when the platform cannot use the default private EPMD port.

Node values must be full name@private-dns-name values. Configuration rejects
localhost, loopback, missing host parts, short names, and a control-plane name
equal to the runner name in production.

The cookie must be a high-entropy secret supplied through the environment. It
must not appear in diagnostics, crash reports, release descriptors, command
output, or generated examples.

The release env/vm configuration fixes the distribution port, enables the
required node name, and fails before application startup when variables are
invalid. Firewall documentation identifies EPMD and both fixed distribution
ports exactly.

### 7.3 Connectivity behavior

The control plane attempts a bounded connection during readiness and on
dispatch. It does not create atoms from arbitrary request data; the runner node
atom is created once from validated boot configuration.

RPC calls normalize:

- nodedown;
- connect timeout;
- erpc timeout;
- remote exception/exit;
- unsupported runner function;
- runner release mismatch.

Errors are redacted and have stable retry classifications. Connectivity and
timeout errors are retryable only where the existing operation is known safe.
The orchestrator must not blindly retry a mutation whose outcome is unknown.

The runner reports release info through diagnostics. There is no dynamic runner
registry in this topology: the statically configured node is considered
available after a successful authenticated BEAM connection, descriptor
verification, and diagnostic probe.

## 8. Runtime configuration

### 8.1 Configuration loading

Create root config/runtime.exs for release-time configuration and keep
config/config.exs limited to environment-independent application structure and
development/test defaults.

Production release boot calls one typed
FavnOrchestrator.ControlPlaneRuntimeConfig loader before the control-plane
supervisor constructs children. If retaining separate
FavnView.ProductionRuntimeConfig and
FavnOrchestrator.ProductionRuntimeConfig modules is clearer, a small
control-plane loader must validate both into one immutable boot result before
either applies changes.

The loader:

1. reads System.get_env/0 once;
2. parses and validates all known values without mutating Application env;
3. accumulates all validation errors where practical;
4. redacts secret names and values appropriately;
5. applies the validated result once;
6. stores a redacted immutable diagnostic summary;
7. starts supervisors and listeners only after success.

Runtime modules must use the frozen typed configuration, not repeatedly call
System.get_env/1. Release tasks use the same parsers for their relevant subset.

Remove every deployment-variable System.get_env call from config/config.exs.
Remove compile-time variation for ports, endpoint URLs, service tokens,
bootstrap credentials, database configuration, runner mode, node target,
limits, and session TTL.

Static framework structure remains compile-time configuration: endpoint adapter,
error renderer modules, PubSub module, fixed secure cookie policy, fixed cookie
salts, LiveView signing salt, static manifest, and dev/test-only routes. These
are not deployment choices or secrets.

### 8.2 PostgreSQL-only configuration

Remove FAVN_STORAGE from production validation, generated examples, build
metadata, documentation, and tests. Remove production storage selection
branches. Production always installs FavnStoragePostgres.Backend and the
PostgreSQL Repo/runtime children.

The control-plane image has one storage composition. Unsupported or missing
storage selection is no longer a possible runtime state.

Keep and validate these PostgreSQL values:

| Environment variable | Requirement |
| --- | --- |
| FAVN_DATABASE_URL | Required secret PostgreSQL URL |
| FAVN_DATABASE_SSL_MODE | Required in production; verify_full is the normal value |
| FAVN_DATABASE_SSL_CA_FILE | Required when the system trust store cannot verify the server; CA certificates are not treated as secrets |
| FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE | Accepted only for explicit localhost development, never by the production release |
| FAVN_DATABASE_POOL_SIZE | Integer 1..200, default 15 |
| FAVN_DATABASE_QUEUE_TARGET_MS | Integer 1..120000, default 50 |
| FAVN_DATABASE_QUEUE_INTERVAL_MS | Integer 1..120000, default 1000 |
| FAVN_DATABASE_TIMEOUT_MS | Integer 1..120000, default 15000 |

The URL password, CA content, query parameters, and host details are redacted
from diagnostics. Readiness reports only connection status, TLS mode, pool
limits, schema version, grant status, and bounded error codes.

### 8.3 Control-plane application variables

Retain or add these variables with typed validation:

| Environment variable | Contract |
| --- | --- |
| FAVN_WORKSPACE_IDS | Required comma-separated stable IDs; non-empty, unique, bounded |
| FAVN_ORCHESTRATOR_API_BIND_HOST | Private bind address, default 0.0.0.0 |
| FAVN_ORCHESTRATOR_API_PORT | Port 1..65535, default 4101 |
| FAVN_ORCHESTRATOR_API_SERVICE_TOKENS | Required versioned service-token set |
| FAVN_ORCHESTRATOR_MANIFEST_COMPRESSED_LIMIT_BYTES | 1 MiB..32 MiB, default 8 MiB |
| FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES | At least compressed limit and at most 128 MiB, default 32 MiB |
| FAVN_ORCHESTRATOR_AUTH_SESSION_TTL | Positive seconds up to 30 days, default 12 hours |
| FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME | Required on first provision; stable normalized username |
| FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD | Required secret on first provision; existing password rules apply |
| FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME | Required bounded display name |
| FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES | Required known role set |
| FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES | 64 MiB..8 GiB, default 512 MiB |
| FAVN_SCHEDULER_ENABLED | Strict boolean, default true |
| FAVN_SCHEDULER_TICK_MS | Positive bounded interval, existing default |
| FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES | Non-negative bounded integer, existing default |
| FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS | 1000..3600000, default 120000 |
| FAVN_RUNNER_RPC_TIMEOUT_MS | 100..120000, default 15000 |
| FAVN_RUNNER_DIAGNOSTICS_TIMEOUT_MS | 100..30000, default 5000 |
| FAVN_RUNNER_AWAIT_TIMEOUT_BUFFER_MS | 0..120000, default 2000 |

Service-token parsing must retain stable token IDs and overlap support. Store
only password hashes in runtime structures after validation where the existing
auth path permits it. Diagnostics report token IDs/count, never token values or
hashes.

### 8.4 Favn View and HTTP edge variables

| Environment variable | Contract |
| --- | --- |
| FAVN_VIEW_PUBLIC_ORIGIN | Required absolute HTTPS origin; HTTP only for explicit localhost development |
| FAVN_VIEW_SECRET_KEY_BASE | Required secret with at least 64 characters |
| FAVN_VIEW_BIND_HOST | Bind address, default 0.0.0.0 |
| FAVN_VIEW_PORT | Port 1..65535, default 4000 |
| FAVN_VIEW_TRUSTED_PROXY_CIDRS | Required private proxy CIDR allowlist when forwarded headers are enabled |
| FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS | 100..30000, default 1000 |
| FAVN_HTTP_MAX_CONNECTIONS | 1..100000, default 1024 per listener |
| FAVN_HTTP_REQUEST_TIMEOUT_MS | 1000..120000, default 30000 |
| FAVN_HTTP_IDLE_TIMEOUT_MS | 1000..300000, default 60000 |
| FAVN_HTTP_BODY_LIMIT_BYTES | 64 KiB..8 MiB, default 1 MiB for ordinary requests |

Forwarded host, port, scheme, and client IP are trusted only when the immediate
peer matches FAVN_VIEW_TRUSTED_PROXY_CIDRS. The public origin is the authority
for generated external URLs. Secure, HTTP-only, SameSite=Lax encrypted session
cookies are fixed production policy.

The reference reverse-proxy guide includes:

- TLS 1.2 or newer;
- WebSocket upgrade forwarding for LiveView;
- request ID propagation;
- the general and manifest-specific request limits;
- matching proxy and application timeouts;
- private routing to the View listener;
- no route from the public listener to the orchestrator API.

### 8.5 Runner variables

The runner reads:

- FAVN_RUNNER_NODE;
- FAVN_CONTROL_PLANE_NODE for validation/diagnostics of the expected peer;
- FAVN_DISTRIBUTION_COOKIE;
- FAVN_BEAM_DISTRIBUTION_PORT;
- ERL_EPMD_PORT when overridden;
- FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS;
- plugin/adapter-specific environment variables declared through existing
  runtime requirements.

Remove FAVN_RUNNER_MODE. The production runner is always a separate distributed
BEAM node. Local test composition uses explicit application configuration, not
a production mode switch.

The baked runner descriptor is artifact data, not an environment override.
Production must not offer an environment variable that changes
runner_release_id or points at a replacement descriptor.

## 9. Secrets and manual rotation

### 9.1 Secret provider contract

Favn reads secrets only from environment variables in this release. It does not
read mounted secret files, call a cloud secret manager, poll for changes, or
hot-reload credentials.

A container platform may resolve a secret-manager reference into an environment
variable. That is an operator/platform concern; Favn still sees a normal
environment value. Applying a new value requires restarting the affected
service or creating a new service revision. Broader secret-provider and
automatic-rotation support is tracked in issue #530.

Secret values include:

- database URL/password;
- distribution cookie;
- orchestrator service-token secrets;
- bootstrap password;
- Favn View secret_key_base;
- runtime-input encryption keys;
- customer plugin/connection credentials.

All logging, diagnostics, validation errors, telemetry, crash metadata, build
metadata, bundles, and image labels must redact them.

### 9.2 Controlled restart procedure

Every planned secret rotation follows:

1. Announce a maintenance window.
2. Put the control plane into draining state.
3. Stop new HTTP run admission, schedule dispatch, manifest activation, and
   runner work submission.
4. Wait for active work up to FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS.
5. Back up PostgreSQL when the rotated material affects encrypted durable data.
6. Change environment variables in the deployment platform.
7. Restart the affected runner and/or control-plane revision.
8. Require liveness and readiness to pass.
9. Perform a smoke run.
10. Resume admission and scheduling.
11. Remove old credentials only after the overlap checks below succeed.

### 9.3 Service-token rotation

FAVN_ORCHESTRATOR_API_SERVICE_TOKENS supports at least two simultaneously valid,
versioned token entries.

Rotation is:

1. Add the new token beside the old token and restart the control plane.
2. Update publisher clients, restart them if needed, and prove the new token.
3. Remove the old token and restart the control plane.
4. Verify audit logs identify the new stable token ID.

The control plane compares hashes in constant-time through the existing
credential boundary. It never persists or logs plaintext tokens.

### 9.4 Runtime-input key rotation

Retain the existing versioned key-ring model:

- FAVN_RUNTIME_INPUT_PIN_KEYS contains retained versioned keys;
- FAVN_RUNTIME_INPUT_PIN_KEY_VERSION selects the key for new writes;
- old keys remain available for reads while PostgreSQL pin inventory references
  them.

Rotation is:

1. Add a new key version while retaining old versions.
2. Set the new current version.
3. Drain and restart the control plane.
4. Verify readiness and new-write diagnostics report the new current version.
5. Use the release-safe inventory/compaction task to prove an old version has no
   remaining pins.
6. Remove that version, restart, and verify readiness.

Startup fails closed if the current version is missing, a key is malformed,
duplicate versions exist, limits are exceeded, or the database references a key
that is not configured.

### 9.5 Session key rotation

Changing FAVN_VIEW_SECRET_KEY_BASE invalidates active browser sessions. The
runbook explicitly tells operators that users will sign in again after the
restart. Durable application state is unaffected.

## 10. Lifecycle, health, and shutdown

### 10.1 Liveness

Liveness answers only whether the release process and health endpoint can
respond. It does not call PostgreSQL, the runner, plugins, or external systems.
It stays healthy during a dependency outage and during draining until shutdown
actually begins.

### 10.2 Control-plane readiness

Readiness is false until all of these checks pass:

- boot configuration validated and frozen;
- Phoenix/View and private API configuration valid;
- PostgreSQL connection succeeds with verified TLS;
- the exact database schema and required grants are present;
- the scheduler is running and its diagnostics are within the existing lag/error
  limits when enabled;
- lifecycle state is accepting work;
- the configured runner node is connected;
- runner diagnostics succeed within the bounded timeout;
- the runner descriptor is valid and runner self-verification passed;
- every active workspace manifest requires the runner's advertised
  runner_release_id;
- the runner reports the required manifest cache/registration health for active
  work.

The readiness response uses stable check names:

- config;
- api;
- view;
- storage;
- schema;
- scheduler;
- lifecycle;
- runner_connection;
- runner_release;
- active_manifests.

Each check is redacted and bounded. The Favn View readiness endpoint delegates
to the public FavnOrchestrator readiness facade in the same BEAM and adds only
View-owned checks.

### 10.3 Runner readiness

Runner readiness is false until:

- runtime configuration is valid;
- the release descriptor is present and self-verifies;
- the runner server and supervisors are alive;
- the lifecycle state accepts work;
- selected plugins/adapters complete their bounded boot preflight;
- diagnostics can report the release identity.

It reports no direct public HTTP endpoint. The control plane queries it over the
runner client boundary. A container-level health command may invoke a local
release RPC/eval that returns success only for the same readiness function.

### 10.4 Admission and drain state

Add a small explicit lifecycle component owned by each runtime:

- :starting;
- :accepting;
- :draining;
- :stopping.

FavnOrchestrator.Lifecycle is the authority for new control-plane admission.
FavnRunner.Lifecycle is the authority for new runner work. State transitions
are monotonic during shutdown. Readiness derives from lifecycle state.

All admission edges consult the control-plane lifecycle:

- run and rerun submission;
- backfill submission;
- scheduler occurrence dispatch;
- manifest activation;
- runner work submission.

Read-only operator queries remain available while draining until listeners
stop.

### 10.5 Control-plane SIGTERM

Supervision and release shutdown must guarantee this order:

1. Enter :draining and make readiness false.
2. Stop accepting new public/private HTTP mutations.
3. Stop scheduler claims and new occurrence dispatch.
4. Stop new runner work dispatch.
5. Allow active run servers and persistence operations to settle.
6. Wait up to FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS.
7. Request cancellation for remaining runner executions.
8. Persist acknowledged terminal states or explicit unknown-outcome/recovery
   state; never invent success.
9. Release/fence durable leases and ownership where safe.
10. Stop API/View listeners.
11. Stop scheduler, orchestrator workers, PubSub, and repositories in dependency
    order.

PostgreSQL remains the authority if the process dies before completion.
Recovery must use existing ownership/fencing rules and must not duplicate
successfully completed work.

Child shutdown timeouts must be longer than the configured drain window where
needed. Replace the current blanket five-second worker shutdown where it can
kill active work before the coordinator acts.

### 10.6 Runner SIGTERM

Runner shutdown:

1. Enters :draining and reports not ready.
2. Rejects new manifest leases, work submissions, and inspections that execute
   code.
3. Allows active workers to finish within the drain window.
4. Returns completed results to the control plane while the connection exists.
5. Uses the existing cancellation path for remaining work at the deadline.
6. Stops workers only after result/cancellation state has been recorded as
   honestly as possible.

The runner never starts new work after draining begins. If the control plane is
unreachable, durable control-plane ownership and fencing determine recovery.

## 11. Release-safe database operations

Move operational behavior out of Mix-only task modules into plain release-safe
modules in favn_storage_postgres. Mix tasks become thin development wrappers.

Provide documented release invocations for:

- FavnStoragePostgres.Release.migrate/0;
- FavnStoragePostgres.Release.verify_schema/0;
- FavnStoragePostgres.Release.grant_runtime/0;
- FavnStoragePostgres.Release.provision_workspace/1;
- FavnStoragePostgres.Release.runtime_input_key_inventory/0;
- FavnStoragePostgres.Release.compact_runtime_input_keys/1;
- FavnStoragePostgres.Release.preflight_upgrade/0.

Each function:

- uses the same runtime database/TLS parser;
- returns or exits with a stable success/error result;
- is idempotent where the underlying operation is idempotent;
- logs migration/provision identifiers but not URLs or secrets;
- refuses to run with the restricted runtime role when elevated privileges are
  required;
- is callable with the control-plane release binary in a one-off container.

Normal application startup calls verify_schema only through readiness and never
migrate.

The migration for runner release identity adds the manifest column, indexes or
checks described above, updates exact schema-readiness expectations, and adds
upgrade/downgrade tests. Database restore drills include the new field.

## 12. Upgrade and rollback runbook

### 12.1 Control-plane upgrade

The supported procedure is:

1. Record currently deployed control-plane image digest, runner image digest,
   runner release descriptor, active manifest IDs, and environment revision.
2. Run preflight_upgrade from the candidate image.
3. Confirm no legacy active manifest or non-terminal legacy run blocks the new
   runner identity contract.
4. Drain admission and scheduling.
5. Take and verify a PostgreSQL backup before a schema migration.
6. Run migrations as a one-off release task.
7. Run grant_runtime when migration changes permissions.
8. Replace the control-plane image by immutable digest.
9. Require all readiness checks.
10. Run login, manifest-read, SQL, and Elixir smoke paths.
11. Resume admission.

### 12.2 Runner code and manifest upgrade

The supported procedure is:

1. Run mix favn.build.runner.
2. Build and push the runner image, recording its immutable OCI digest.
3. Publish the aligned manifest bundle inactive.
4. Drain the control plane.
5. Replace the runner image by digest.
6. Require runner self-verification and runner readiness.
7. Activate the aligned manifest for every configured workspace.
8. Require control-plane readiness.
9. Run smoke execution.
10. Resume admission.

With one runner, work is unavailable between removing the old runner and
activating the new aligned manifests. Queued durable work remains in
PostgreSQL.

### 12.3 Manifest-only upgrade

The supported procedure is:

1. Run mix favn.build.manifest with the deployed runner-release.json.
2. Require an exact runtime fingerprint match.
3. Publish the manifest bundle inactive.
4. Activate the new manifest.
5. Verify readiness and run a smoke execution.

The runner container is not restarted and its OCI digest does not change.

### 12.4 Rollback

A rollback is allowed only when the old control-plane image understands the
current database schema. The migration metadata/runbook marks compatibility
explicitly.

Before declaring an upgrade successful, retain:

- the previous control-plane image digest;
- the previous runner image digest;
- the previous runner descriptor;
- previous active manifest IDs;
- the previous environment revision;
- the verified database backup.

Runner/manifest rollback drains work, redeploys the old runner image, verifies
its old runner_release_id, and reactivates the old manifest. A new runner may
not execute an old manifest that requires a different release ID.

## 13. Observability and audit

Add bounded telemetry and structured logs for:

- runtime configuration accepted/rejected;
- node connection/disconnection;
- runner diagnostic latency and result;
- expected and actual runner release ID on mismatch;
- manifest publication/staging/activation;
- activation rejection reason;
- lifecycle transition and drain duration;
- active work at drain start/deadline;
- cancellation acknowledgement/unknown outcome;
- release task start/result;
- schema/preflight result.

IDs may be logged: workspace, manifest version, deployment, runner release,
image digest when supplied by operator metadata, token ID, and migration ID.
Secrets, environment values, SQL text, runtime inputs, cookies, database URLs,
and full exception payloads may not be logged.

Expose runner release ID and manifest requirement in operator diagnostics,
manifest/deployment views, and the run detail read model. Keep responses bounded
and use existing redaction helpers.

## 14. Remove superseded production surfaces

After the new artifacts and acceptance path work:

- delete mix favn.build.web;
- delete mix favn.build.orchestrator;
- delete mix favn.build.single;
- delete mix favn.bootstrap.single;
- delete Favn.Dev.Build.Web, Orchestrator, and Single;
- delete single-node launcher scripts and their state fields;
- remove FAVN_STORAGE;
- remove FAVN_RUNNER_MODE;
- remove metadata that describes web/orchestrator outputs as future artifacts;
- remove the production local-runner fallback;
- remove the local full-umbrella RuntimeSource/RuntimeWorkspace materialization
  and source-built control-plane launch path after Compose acceptance passes;
- remove local operator/runner host-process launch code and host-bound BEAM
  distribution plumbing superseded by the Compose network;
- remove stale tests, docs, guides, AI breadcrumbs, examples, and CLI error
  clauses for those commands.

Do not keep aliases or deprecation shims. Favn is pre-v1 and repository policy
requires stale forms to be removed.

The final public production command surface is:

- mix favn.build.runner;
- mix favn.build.manifest;
- mix favn.publish;
- mix favn.activate;
- the maintainer control-plane image build;
- release-safe PostgreSQL operations.

Local development commands remain local tooling and must not be documented as
production deployment artifacts.

## 15. Implementation ownership and file-level work

### 15.1 favn_core

Implement:

- Favn.RunnerRelease descriptor, serializer, identity, validation, and types;
- canonical executable BEAM digest support with deterministic tests,
  post-strip parity, and redacted absolute-literal rejection;
- runtime root/closure value types that do not depend on Mix;
- required_runner_release_id in manifests and versions;
- compatibility/schema/contract version changes;
- required runner ID in RunnerWork, RunnerResult, RunnerEvent, and applicable
  inspection contracts;
- stable errors and public documentation.

Core must not know Docker, cloud platforms, application environment, PostgreSQL,
or local filesystem layout beyond accepting explicit binaries/metadata for pure
hashing.

### 15.2 favn_authoring

Implement:

- collection of runtime roots from built manifests and execution packages;
- extraction of runtime-input resolver references;
- the aligned manifest build entrypoint that requires a validated runner
  descriptor;
- useful, redacted mismatch diagnostics;
- propagation of the required runner release into the canonical manifest.

Authoring owns compilation-time discovery; it does not build containers or call
the orchestrator.

### 15.3 favn_local and public Mix tasks

Refactor existing Favn.Dev.Build.Runner instead of layering another unrelated
builder over it.

Implement:

- project compilation and runtime root closure;
- deterministic descriptor input collection;
- runner descriptor generation;
- manifest bundle generation through favn_authoring;
- relocatable OCI build context;
- bundle hashes and operator notes;
- public tasks for build.runner, build.manifest, publish, and activate;
- Docker Engine and Compose v2 prerequisite/feature probes;
- GHCR version-tag resolution, pull, RepoDigest validation, OCI-label and
  contract verification, and atomic control-plane install state;
- project-scoped Compose file, network, volume, service-name, environment, and
  loopback-port generation without embedding secrets in command arguments;
- Compose lifecycle ownership for install, dev, reload, status, logs, stop,
  reset, doctor, and diagnostics;
- local runner image caching keyed by runner_release_id and replacement only
  after a successful drain;
- one-shot local migration, grant, verification, and provisioning through the
  installed release image;
- precise CLI argument parsing and stable exit messages;
- removal of the full Favn umbrella runtime copy/materialization and local
  View/Orchestrator/storage compilation path;
- removal of skip_web_install, skip_runtime_deps_install, and skip_tool_checks
  public options;
- removal of superseded build/bootstrap tasks.

Filesystem writes use existing Paths/State helpers. Failed builds write to a
temporary directory and atomically rename only after every validation and file
hash succeeds. A failed build must not leave a directory that appears
publishable.

favn_local may invoke Docker and assemble OCI/Compose inputs, but it must not
gain a runtime dependency on favn_view, favn_orchestrator, or
favn_storage_postgres. It treats the installed control plane as an external OCI
artifact and interacts with it through release commands and public HTTP/health
contracts. The public favn package includes only authoring, local-tooling,
runner-build, and shared-contract package dependencies needed by a customer
project; it does not use package dependencies to pull in the control-plane
applications.

### 15.4 favn_runner

Implement:

- production runtime config for the separate BEAM node;
- FavnRunner.ReleaseVerifier before Runner Server startup;
- FavnRunner.release_info/0;
- descriptor details in bounded diagnostics/readiness;
- runner lifecycle/drain coordinator;
- exact runner release checks on registration, lease, work, runtime input
  resolution, and relation inspection;
- stable mismatch results echoed to orchestrator;
- release assembly hooks used by the generated customer context.

### 15.5 favn_orchestrator

Implement:

- PostgreSQL-only production boot composition;
- BeamNode runner client and static remote-node options;
- runner release-aware diagnostics and readiness;
- staged publication and release-aware activation;
- required runner release propagation into submission, snapshots, work,
  recovery, results, read models, and telemetry;
- control-plane lifecycle/drain coordinator;
- mutation admission checks;
- public facade functions used by Favn View;
- service-token overlap and audit behavior;
- bounded errors suitable for API mapping.

The orchestrator must not acquire a runtime dependency on favn_runner.

### 15.6 favn_storage_postgres

Implement:

- migration and schema validation for required_runner_release_id;
- Ecto schema/query/codec/operator-read changes;
- historical-manifest compatibility rules;
- release-safe migrate, verify, grant, provision, key inventory/compaction, and
  upgrade preflight modules;
- PostgreSQL readiness/diagnostics with exact schema and grants;
- backup/restore and migration regression coverage.

### 15.7 favn_view

Implement:

- removal of the direct favn_storage_postgres dependency from favn_view;
- runtime endpoint configuration;
- trusted proxy and forwarded-header policy;
- public origin and secure-session configuration;
- readiness through the public orchestrator facade;
- display of manifest-required and connected runner release IDs in relevant
  operator views;
- drain-aware mutation error presentation.

View must not learn runner client, persistence, Repo, or storage-adapter
details. The root control-plane release composes favn_storage_postgres as an
explicit release application instead of using a View dependency to pull it in.

### 15.8 root release and container files

Implement:

- favn_control_plane Mix release definition with an explicit application set;
- rel/env and vm arguments for validated node/distribution configuration;
- release overlays for health/release tasks;
- control-plane multi-stage Dockerfile and ignore file;
- pinned builder/runtime base references;
- deterministic control_plane_build_id input collector and human-auditable
  descriptor;
- a dedicated control-plane GitHub Actions workflow with repository GITHUB_TOKEN
  packages:write permission, GHCR lookup/reuse, immutable build/SHA/version tag
  rules, provenance, and SBOM;
- CI image build, start, scan, and content inspection only when the production
  control-plane input identity changes;
- release automation that aliases a runner/dev-tooling-only Favn version to the
  existing compatible control-plane digest without rebuilding it;
- a platform-neutral deployment contract and security guide covering immutable
  digests, environment injection, health, shutdown, stable private DNS, required
  ports, reverse proxying, PostgreSQL, and the trusted-network limitation.

Favn does not publish platform networking, firewall, load-balancer, or
infrastructure-as-code resources in this issue. Operators implement those
resources for their chosen platform and validate them against the documented
contract.

## 16. Verification matrix

### 16.1 Unit tests

Add focused tests for:

- canonical runner descriptor JSON and ID;
- every malformed descriptor field;
- path-independent canonical BEAM digest, pre/post release-strip parity, and
  absolute executable path-literal rejection;
- runtime root selection and transitive import closure;
- explicit dynamic modules/applications;
- SQL-only versus Elixir/runtime change classification;
- manifest required-runner serialization, rehydration, hashing, and mismatch;
- runner work/result contract validation;
- environment parser bounds and redaction;
- BeamNode RPC normalization and timeouts;
- lifecycle state transitions;
- release task return shapes;
- deterministic control-plane input records and build identity;
- production dependency-closure lock filtering;
- GHCR version-tag/reference and RepoDigest parsing;
- Compose project/service/network/volume naming and loopback port generation;
- local change classification into manifest-only versus runner replacement;
- supported-host/architecture validation;
- pinned-checkout metadata and rejection of floating/unresolved Favn revisions;
- separation between official GHCR resolution and internal candidate-image
  injection.

### 16.2 App integration tests

Add tests for:

- PostgreSQL migration from the immediately previous schema;
- old manifest history readable but not activatable;
- current manifests requiring a valid runner release ID;
- activation rejected while runner is offline;
- activation rejected for the wrong runner release;
- activation succeeds for the exact runner release;
- dispatch and runner both reject a forged mismatch;
- run snapshots persist and restore runner release ID;
- recovery refuses an unbound non-terminal legacy run;
- multiple workspace active-manifest readiness during controlled code upgrade;
- service-token overlap across two boot configurations;
- runtime-input key add/switch/remove rules;
- View calls only the public facade for readiness and operations;
- favn_local reaches the control plane only through release commands and public
  HTTP/health contracts, without a runtime control-plane app dependency.

### 16.3 Build tests

In temporary consumer projects, prove:

1. An Elixir asset plus helper is discovered and fingerprints correctly.
2. A runtime-input resolver is included even for an otherwise SQL asset.
3. A dynamic helper is included through runner_build extra_modules.
4. A pure SQL edit changes manifest content hash but not runner_release_id.
5. A pipeline/schedule-only edit changes manifest but not runner_release_id.
6. An Elixir asset edit causes build.manifest to fail with
   runner_rebuild_required.
7. A helper, resolver, plugin, or lock edit causes the same failure.
8. Rebuilding the runner after the edit creates a new runner_release_id and an
   aligned manifest.
9. Copying the generated context to another absolute directory still builds.
10. No generated JSON/text file leaks the project absolute path or a secret.
11. A control-plane source/config/release input edit changes
    control_plane_build_id.
12. Runner-only, favn_local-only, authoring-only, test-only, example-only, and
    documentation-only edits leave control_plane_build_id unchanged.
13. A root lock change affecting only the runner dependency closure leaves the
    control-plane ID unchanged; a reachable production dependency edit changes
    it.
14. Existing build-<id> GHCR metadata selects digest reuse and skips the image
    builder; missing or invalid metadata fails or builds as specified.
15. A pinned path-dependency consumer vendors the runner closure so its copied
    build context succeeds after both the consumer and Favn checkouts are
    removed.
16. A floating or unidentifiable Favn source revision fails the production
    runner build with targeted guidance.
17. The repository-only --load task produces the expected local candidate tag
    and image ID; no public install CLI option can select it.

### 16.4 Local Compose acceptance

In a clean customer project with Docker Engine and Compose v2, prove:

1. mix favn.install pulls the private version tag, records its RepoDigest, and
   does not compile View, Orchestrator, storage, or Phoenix assets.
2. Repeating install uses the locally cached exact digest; --force revalidates
   and repulls without changing a valid digest.
3. Missing Docker, missing Compose v2, missing GHCR authentication, missing
   version tag, label mismatch, and contract mismatch each fail with a targeted
   error and no ready install state.
4. mix favn.dev starts digest-pinned PostgreSQL, the exact installed control
   plane, and the locally built customer runner on one private network.
5. No source tree is mounted into either running release and no BEAM
   distribution or PostgreSQL port is published to the host.
6. Only the documented View/private API ports bind to 127.0.0.1.
7. Local migration/provisioning completes before control-plane startup, full
   readiness proves runner alignment, and the UI executes SQL and Elixir work.
8. A SQL/manifest-only reload leaves both image IDs and container start times
   unchanged.
9. A customer Elixir/runtime edit builds a new runner ID and replaces only the
   drained runner container before activating its aligned manifest.
10. A blocked drain leaves the old runner and active manifest untouched.
11. stop preserves the PostgreSQL volume; a subsequent dev start restores
    state.
12. reset requires explicit --yes, removes only the current project-scoped
    containers, volume, generated runner images, and .favn state, and cannot
    target another Compose project.
13. status, logs, doctor, and diagnostics report bounded useful state and never
    expose generated secrets or Docker credentials.
14. Linux amd64 and amd64 WSL2 feature probes pass; unsupported host/container
    architectures fail before pulling or starting services.

### 16.5 Production container acceptance

Run one golden acceptance scenario in clean containers:

1. Start PostgreSQL 18.
2. Run migration, runtime grant, and workspace provisioning from the
   control-plane image.
3. Build the canonical customer fixture's runner image.
4. Start one runner with a fixed private name/port/cookie.
5. Start one control plane with Favn View and Orchestrator in the same BEAM.
6. Prove no favn_runner application/module is loaded in the control plane.
7. Prove readiness includes PostgreSQL and remote runner release checks.
8. Sign in through the View listener.
9. Publish packages and a staged manifest through the private API.
10. Activate it and execute SQL and Elixir assets.
11. Restart both containers and prove durable state plus new execution.
12. Publish and activate a SQL-only update without changing the runner image
    digest.
13. Try to publish/activate a manifest with a forged release requirement and
    prove rejection.
14. Build changed Elixir code against the old descriptor and prove the local
    build fails.

The acceptance environment contains no mounted repository and no Mix runtime
inside final images.

### 16.6 Shutdown acceptance

Test:

- idle control-plane SIGTERM;
- active SQL work control-plane SIGTERM;
- active Elixir work runner SIGTERM;
- work completing within the drain window;
- work exceeding the drain window and receiving cancellation;
- control-plane loss during runner completion;
- runner loss during dispatch;
- restart/recovery without duplicate success or silently lost terminal state.

Assert readiness flips before new admission is rejected and that persisted
outcomes are honest.

### 16.7 Security and edge acceptance

Test:

- missing/blank/malformed required environment variables fail before listeners;
- FAVN_STORAGE and FAVN_RUNNER_MODE have no production effect because they are
  absent from the contract;
- secrets never appear in logs, diagnostics, bundles, release metadata, or
  image history;
- untrusted forwarded headers are ignored;
- trusted proxy origin/scheme/client-IP handling;
- secure session cookies;
- request and decompression limits;
- LiveView WebSocket through the reference proxy;
- private API authentication and service-token overlap;
- BEAM ports bind as documented and are absent from the public proxy;
- PostgreSQL plaintext is rejected in production;
- image runs as non-root with read-only root filesystem.

### 16.8 Upgrade, rollback, and rotation acceptance

Perform and document:

- compatible control-plane upgrade with external migration;
- compatible control-plane rollback;
- runner plus manifest upgrade and rollback;
- manifest-only upgrade;
- service-token add/switch/remove with restarts;
- runtime-input key add/switch/inventory/remove with restarts;
- secret_key_base change and expected session invalidation.

### 16.9 Supply-chain checks

CI must:

- run mix hex.audit;
- run the existing Elixir static/security checks;
- scan final OCI images for known high/critical vulnerabilities and fail unless
  an explicit repository-tracked exception exists;
- inspect image contents for source roots, Mix, dependency caches, credentials,
  writable broad directories, and unexpected applications;
- record and test immutable image digests and OCI metadata;
- verify base images are pinned by digest.
- prove control-plane image jobs are skipped when control_plane_build_id is
  unchanged and that version-tag promotion reuses the existing digest.
- prove pull-request credentials cannot publish packages, main cannot publish
  before its exact-commit acceptance succeeds, and release promotion cannot
  overwrite an existing version tag.

## 17. Documentation changes

Update in the same implementation:

- README.md;
- docs/FEATURES.md;
- docs/ROADMAP.md;
- docs/production/single_node_contract.md, renamed to describe the
  control-plane/runner topology;
- docs/production/single_node_acceptance_matrix.md, renamed to the issue #522
  acceptance matrix;
- docs/production/postgresql_operator_runbook.md;
- docs/production/public_api_boundary.md;
- docs/structure/favn.md;
- docs/structure/favn_orchestrator.md;
- docs/structure/favn_view.md;
- add runner release, deployment, upgrade/rollback, proxy/network, and manual
  secret-rotation operator guides;
- add a local Docker/Compose development guide covering prerequisites, private
  GHCR login, install, startup order, private networking, image reuse, reload
  classification, persistent volumes, stop/reset, and troubleshooting;
- document Linux amd64 and amd64 WSL2 as the supported local host matrix and
  state the unsupported native-Windows, arm64, macOS-emulation, and Podman
  limitations explicitly;
- document the pre-Hex pinned-checkout/path-dependency consumer setup and prove
  that generated runner contexts no longer require the checkout or private Git
  credentials;
- document the repository-only unpublished candidate build/acceptance path
  separately from public mix favn.install;
- document the PR, main, and release CI event matrix, including which jobs may
  publish packages;
- document that control-plane version tags may alias an older compatible image
  digest when a Favn release changes only runner or development tooling;
- document which repository inputs affect control_plane_build_id and how CI
  reports build-versus-reuse decisions;
- favn HexDocs guides and public Mix task docs;
- public moduledocs/typespecs/examples;
- Favn.AI breadcrumbs and cheatsheet entries;
- favn_local README and any generated operator notes.

Documentation must use one consistent vocabulary:

- control plane: View plus Orchestrator in one BEAM;
- runner: customer-built separate BEAM;
- runner contract version: protocol compatibility;
- runner release ID: exact customer runtime compatibility;
- manifest release: independently publishable manifest plus execution packages;
- image digest: exact OCI artifact selected by the operator.
- control-plane build ID: deterministic identity of inputs that can change the
  official control-plane image.

Examples use environment variable placeholders and immutable image digests.
They do not contain real credentials, public BEAM endpoints, deprecated
commands, or alternate production storage choices.

## 18. Implementation order and merge gates

Implement in this order so every intermediate change has a coherent owner:

1. Add runner descriptor/identity and deterministic fingerprint primitives in
   favn_core.
2. Add required_runner_release_id to manifest and runner contracts, with shared
   fixtures.
3. Add PostgreSQL migration, codecs, exact readiness, and release-safe tasks.
4. Make Orchestrator publication, activation, runs, and recovery release-aware.
5. Make Runner self-verify and reject mismatched work.
6. Replace LocalNode with the production BeamNode client and remote readiness.
7. Implement boot-time env-only configuration and PostgreSQL-only composition.
8. Add lifecycle/drain behavior.
9. Refactor runner/manifest builders and add publish/activate tasks.
10. Add the favn_control_plane release, deterministic control-plane build
    identity, GHCR workflow, and both OCI build paths.
11. Replace local source materialization/host processes with the mandatory
    Docker Compose install/dev/reload/operations contract and golden local
    acceptance.
12. Run the complete production container acceptance, security, rotation, and
    upgrade/rollback matrix.
13. Remove superseded single/web/orchestrator build/bootstrap and source-built
    local control-plane surfaces.
14. Update all documentation and perform final stale-reference searches.

Each step requires:

- focused tests at the owning app;
- public docs/typespecs for public functions;
- no new cross-app boundary violation;
- mix format;
- mix compile --warnings-as-errors;
- the narrowest app test slice before umbrella verification.

Before issue completion run:

    mix format
    mix compile --warnings-as-errors
    mix test --no-compile --timeout 1200000
    mix test.acceptance
    mix test.slow
    elixir scripts/check_test_tag_tiers.exs

## 19. Reviewer checklist

A reviewer should reject the implementation if any answer is no:

### Architecture

- [ ] View and Orchestrator run in one control-plane BEAM.
- [ ] View uses only the public Orchestrator facade.
- [ ] Runner is a separate customer-built BEAM/image.
- [ ] Control plane has no runtime dependency on favn_runner.
- [ ] PostgreSQL is the only production backend.
- [ ] BEAM/PostgreSQL/private API ports are documented as private-only.

### Artifact truthfulness

- [ ] The control-plane output is a runnable minimal OCI image.
- [ ] The generated runner context builds and starts a runnable release.
- [ ] Final images contain neither Mix nor the repository/source tree.
- [ ] Artifact metadata is relocatable and contains immutable identities.
- [ ] The user, not Favn, builds/pushes the final customer runner image.
- [ ] GHCR is the canonical control-plane registry and production examples use
      its immutable digest.
- [ ] Unchanged control-plane input identity reuses the existing digest without
      running an image build.

### Manifest/runner alignment

- [ ] runner_release_id is deterministic and path-independent.
- [ ] The runner self-verifies its baked descriptor.
- [ ] Manifest identity includes required_runner_release_id.
- [ ] SQL-only edits keep the runner release ID.
- [ ] Relevant runtime edits make manifest-only build fail.
- [ ] Orchestrator and Runner independently reject mismatches.
- [ ] Run snapshots pin the exact release identity.
- [ ] OCI digest remains separate from logical runner release identity.

### Configuration and secrets

- [ ] Production configuration is read from env once at boot.
- [ ] No deployment env is read during config/config.exs compilation.
- [ ] Invalid config fails before supervisors/listeners.
- [ ] FAVN_STORAGE and FAVN_RUNNER_MODE are removed.
- [ ] Secrets are env-only and never logged or baked.
- [ ] Manual overlap/restart rotation is tested and documented.

### Operations

- [ ] Migrations/provisioning run from the release without Mix.
- [ ] Startup validates but never auto-migrates.
- [ ] Readiness proves the remote runner release and exact PostgreSQL schema.
- [ ] SIGTERM stops admission and drains before killing work.
- [ ] Upgrade and rollback work by immutable image digest.
- [ ] The golden two-container acceptance passes in a clean environment.
- [ ] Docker Engine and Compose v2 are mandatory and the golden local
      PostgreSQL/control-plane/runner Compose acceptance passes.
- [ ] mix favn.install pulls/verifies the control plane and never builds it.
- [ ] Local SQL-only reload rebuilds no image; local runtime-code reload replaces
      only a successfully drained runner.
- [ ] Local BEAM/PostgreSQL ports remain private to the Compose network and
      browser/API ports bind only to loopback.
- [ ] Supported local hosts are limited and tested as Linux amd64 and amd64
      WSL2 with Docker Engine/Desktop plus Compose v2.
- [ ] The pinned-checkout consumer path vendors a credential-free relocatable
      runner context and does not compile the control plane.
- [ ] Unpublished candidate-image injection exists only behind repository test
      and maintainer boundaries, not the public install CLI.
- [ ] PRs build/test but cannot publish changed candidates; main publishes only
      after exact-commit acceptance; release qualification alone adds the Favn
      version tag.

### Cleanup and documentation

- [ ] Old build.web, build.orchestrator, build.single, and bootstrap.single
      surfaces are deleted.
- [ ] Full-umbrella local runtime materialization and source-built/host-process
      control-plane paths are deleted.
- [ ] All public docs and AI breadcrumbs describe the same contract.
- [ ] Stale-reference searches find no production recommendation for SQLite,
      local in-process runners, or the removed build commands.
- [ ] Full required verification passes.
