# Elastic runners

Favn exposes demand and accepts runners; it does not manage infrastructure.
Pool names are arbitrary logical environment names such as `duckdb`,
`pure_elixir`, or `gpu`, not fixed sizes.

Configure a pool as elastic or resident. An elastic runner claims one task at a
time, waits the configured idle grace after an empty claim, performs one final
claim, and exits when that is also empty. A resident runner waits indefinitely.
Any elastic pool requires a raw
`FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN`; resident-only deployments may omit
it. Favn assigns this credential only the `capacity_reader` role. The general
`FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` variable cannot grant that role.

External infrastructure reads:

```text
GET /internal/runner-demand/<runner-pool>/<runner-release-id>
Authorization: Bearer <capacity_reader token>
```

The exact response is:

```json
{"outstanding": 7}
```

The value includes queued and active tasks. The scaler must account for
already-running jobs. The equivalent OpenMetrics route appends `/metrics`.
Operator diagnostics are at
`GET /api/orchestrator/v1/runner-capacity`. That bounded overview reports
whether it was truncated. Before removing one old runner definition, use the
exact-partition authority at
`GET /api/orchestrator/v1/runner-capacity/<runner-pool>/<runner-release-id>`
and require `drained: true`.

Production nodes require mutual-TLS distributed BEAM plus a high-entropy
cookie. Start the VM with:

```text
-proto_dist inet_tls
-ssl_dist_optfile /absolute/path/ssl_dist.config
-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9100
```

The TLS file must verify peers on both client and server, require client
certificates on the server, and use a control-plane certificate whose SAN
matches the stable DNS host portion of `FAVN_CONTROL_PLANE_NODE`; OTP uses that
host as TLS SNI. Runner certificates must chain to the trusted CA. Runners start
as `undefined@<stable-host-alias>` dynamic nodes, connect only to the control
plane, and do not listen for inbound distribution. Rotate capacity tokens with
an overlap through `FAVN_ORCHESTRATOR_CAPACITY_READER_PREVIOUS_TOKEN`, move
scalers to the new primary value, and then remove the previous value. Rotate
certificates and the cookie by introducing a new release partition, draining
the old one, and then removing its credentials.

Reference adapters live in [`deployment/`](../../deployment/README.md).
Azure Container Apps and Kubernetes are examples, not dependencies or
qualified managed-platform claims. ECS tasks, Nomad batch jobs, VM scalers, and
ordinary process supervisors can implement the same demand/start/self-exit
contract. Resident pools map to services or supervised processes; elastic
pools map to disposable jobs.

Current implementation references:

- [OTP 29 TLS distribution](https://www.erlang.org/doc/apps/ssl/ssl_distribution.html)
- [Azure Container Apps jobs](https://learn.microsoft.com/en-us/azure/container-apps/jobs)
- [Microsoft.App jobs resource](https://learn.microsoft.com/en-us/azure/templates/microsoft.app/jobs)
- [Microsoft.App containerApps resource](https://learn.microsoft.com/en-us/azure/templates/microsoft.app/containerapps)
- [KEDA 2.20 Metrics API scaler](https://keda.sh/docs/2.20/scalers/metrics-api/)
- [KEDA 2.20 ScaledJobs](https://keda.sh/docs/2.20/concepts/scaling-jobs/)
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
