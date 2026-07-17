# Manifest Scalability Baseline

This report records the historical issue #483 schema-7 baseline that motivated
the production change. Schema 8 now implements the recommended compact index
and immutable execution packages; the table below remains before-change
evidence rather than a description of the current manifest shape.

## Assumptions and scope

- The deterministic SQL-heavy fixture is calibrated to the reported project:
  66 assets produce 4.54 MB of canonical JSON, close to the observed 4.5 MB.
- Each asset has 14 SQL projection columns, a 14-column output contract, two
  checks, an incremental materialization, session requirements, metadata, and a
  linear dependency edge.
- Results are one isolated sample per size on Elixir 1.20.2 / OTP 28, x86-64,
  with 24 schedulers. Timings are evidence, not a stable performance guarantee.
- Peak process memory is sampled every 100 ms. Referenced binaries are sampled
  every second and at phase boundaries. These are BEAM process measurements,
  not operating-system RSS.
- The harness measures fixture construction, `Favn.Manifest.Version.new/2`,
  canonical encoding, gzip, SHA-256, JSON decode, decoded flat-heap estimate,
  and field attribution. Storage-adapter persistence and runner registration
  remain separate end-to-end measurements for the package-design phase.

The current harness uses schema 8. Running it after compiling the repository
measures the compact index rather than reproducing the historical table:

```bash
MIX_ENV=test mix run --no-compile scripts/measure_manifest_scalability.exs \
  --assets 66,300,1000 > /tmp/favn-manifest-scalability.json
```

The 6,600-asset case is opt-in rather than a default because the measured
1,000-asset memory curve projects beyond the safe capacity of common developer
machines.

## Results

| Assets | Canonical JSON | Gzip | Gzip / JSON | Decoded flat heap | Version time | Version peak process | Referenced binaries at observed peak | Encode | Decode |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 66 | 4.54 MB | 0.48 MB | 10.5% | 16.6 MB | 1.31 s | 28.0 MB | 9.3 MB | 0.18 s | 0.09 s |
| 300 | 21.03 MB | 2.17 MB | 10.3% | 76.8 MB | 6.89 s | 247.3 MB | 91.0 MB | 0.84 s | 0.26 s |
| 1,000 | 70.54 MB | 7.24 MB | 10.3% | 257.6 MB | 25.24 s | 813.2 MB | 144.5 MB | 3.80 s | 1.92 s |

The 32 MiB expanded publication budget supports approximately 475 assets at
the measured 1,000-asset density. The 8 MiB compressed budget would support
more than 1,100 assets, so the expanded budget is the intentional limiting
guard. This confirms that the issue #483 transport fix is suitable for a few
hundred assets, not thousands.

## 6,600-asset projection

A direct 6,600-asset sample was not run on the 7.6 GiB measurement host because
the 1,000-asset result already makes an out-of-memory failure plausible. A
simple proportional projection from the largest real sample gives:

| Metric | Projected value |
| --- | ---: |
| Canonical JSON | 466 MB |
| Gzip transfer | 47.8 MB |
| Decoded heap | 1.70 GB |
| Manifest versioning | 167 s |
| Version worker peak process memory | 5.37 GB |

This is a projection, not a measured 6,600-asset result. It excludes the rest
of the BEAM, orchestrator persistence, runner registration, concurrent work,
and operating-system overhead.

## What occupies the manifest

At 1,000 assets, approximate JSON field-value attribution is:

| Area | Bytes | Share of canonical JSON |
| --- | ---: | ---: |
| SQL execution payloads | 69.19 MB | 98.1% |
| Main compiled templates | 57.59 MB | 81.6% |
| Main template nodes alone | 56.25 MB | 79.7% |
| Checks, including compiled check templates | 7.34 MB | 10.4% |
| Contracts | 3.05 MB | 4.3% |
| Main raw SQL field | 1.12 MB | 1.6% |
| Graph | 0.32 MB | 0.5% |

The dominant problem is not raw SQL text or the dependency graph. It is the
verbose JSON representation of already-compiled template nodes, followed by
compiled check templates. Removing small duplicate identity or graph fields
cannot materially change the production curve.

## Implemented outcome

Gzip should remain the publication transport optimization for current
few-hundred-asset projects. Raising the monolithic request limits again is not
the production scaling strategy: it would leave versioning, decode, storage,
registration, and memory proportional to the complete catalogue.

Schema 8 implements a content-addressed manifest index plus immutable per-asset
execution packages:

1. Keep dependencies, schedules, catalogue metadata, and each execution-package
   hash in a small canonical index.
2. Put the current manifest-owned SQL execution payload, checks, contract, and
   compiled template IR in an immutable package addressed by its canonical
   content hash.
3. Upload only missing packages, then atomically publish or activate the index
   after every package is present and verified.
4. Load one package for one selected SQL work item instead of registering the
   entire decoded catalogue. A runner cache remains optional future tuning.
5. Preserve packages referenced by pinned manifests and runs; garbage collect
   only unreferenced packages.

This split fixes the scaling boundary even if every individual package retains
the existing verbose template IR. A compact tagged template wire codec could
still reduce storage and cold-fetch cost, but it should be evaluated inside one
package first. Recompiling templates from source during publication or runner
load is not recommended: it moves compiler cost and compiler-version coupling
into the runtime path.

Current regression gates are:

- a directly measured 300-asset index below 2 MiB and a conservative 6,600-asset
  projection below the 32 MiB expanded publication limit;
- no-op publication transfers no execution packages;
- activation remains atomic and pinned runs remain reproducible; and
- runner package memory grows with admitted concurrent work rather than total
  catalogue size.

Production retention, garbage collection, cold package-read measurements, and
whether a bounded runner-local cache is worthwhile remain explicit follow-up
work rather than compatibility layers in schema 8.
