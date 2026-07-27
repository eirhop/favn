# Kubernetes and KEDA reference

Copy `elastic-runner.yaml` once per exact pool/release and replace all
placeholders. Install KEDA 2.20 or revalidate the manifest against the version
you operate. The runner entrypoint starts
`RELEASE_NODE=undefined@<FAVN_RUNNER_NODE_HOST_ALIAS>`. OTP assigns a bounded
dynamic name after the runner explicitly connects outward; pod identity never
becomes an atom or node name and the runner accepts no inbound BEAM connection.

Create `favn-distribution` with a `cookie` key and
`favn-distribution-tls` with `ssl_dist.config`, CA, certificate, and private-key
files referenced by that options file. The templates mount the TLS secret
read-only at `/etc/favn`; use your secret provider rather than committing
credentials. The control-plane certificate SAN must match the DNS host portion
of `FAVN_CONTROL_PLANE_NODE`. Runner certificates are client credentials signed
by the trusted CA.

The endpoint reports queued plus assigned work. KEDA's explicit `default`
ScaledJob strategy subtracts running Jobs from that metric, so an already
started runner does not cause another Job to be created. Keep this strategy
unless the demand contract changes.

Use `resident-runner.yaml` only for pools configured as resident. Extend the
network policy for your DNS implementation, API endpoint, certificate/secret
provider, image registry, telemetry sink, and data-system egress.

Keep old and new ScaledJobs during rollout/rollback. Delete the old object only
after Favn reports that exact partition drained. A Kubernetes Job can start the
program more than once even with `backoffLimit: 0`; Favn's durable claim and
assignment fence provide correctness.

These manifests were not qualified on a managed cluster in this PR.
