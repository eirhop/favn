# Elastic runner deployment contract

These are reference adapters around Favn's provider-neutral contract. Favn
does not create, resize, or delete infrastructure.

For each exact `{runner_pool, runner_release_id}` partition:

- run one independently scalable job definition;
- read `GET /internal/runner-demand/<pool>/<release>` with a
  `capacity_reader` bearer token;
- use JSON field `outstanding` and target value `1`;
- start runners with one slot, outbound access to the control plane, mutual-TLS
  distributed BEAM, and the exact pool/release identity;
- set infrastructure retries to zero and rely on durable Favn claims/fences;
- set the hard timeout above runner maximum uptime + maximum task duration +
  shutdown grace;
- use process exit as the scale-down signal.

During rollout or rollback, keep old and new definitions together. Remove a
definition only when
`/api/orchestrator/v1/runner-capacity/<pool>/<runner-release-id>` reports that
exact partition as `drained`. The collection endpoint is a bounded operator
overview and explicitly reports `partition_limit` plus `truncated`; it is not
the removal authority for a partition omitted from that overview.

The Azure and Kubernetes files are reviewed reference templates, not a claim of
managed-platform qualification. Substitute secrets through the platform secret
store; never commit them.
