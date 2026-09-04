# SQL execution package compaction measurements

Point-in-time measurements for [PR #697](https://github.com/eirhop/favn/pull/697),
4 September 2026. Baseline: `5f3c163e64f96c2b524c445b548f1177708c0bcd`.
The committed [raw measurements](sql_package_compaction.json) contain every
sample, codec timing, memory observation, and 1 MiB crossing bracket.

## Result

Adjacent literal text coalescing moves the ordinary-query 1 MiB package boundary
from **291 columns / 12,059 source bytes** to **7,980 columns / 348,959 source
bytes**: about **29 times more SQL** in this fixture. The 5,000-column fixture
contains 217,839 source bytes and produces a 655,340-byte package, safely below
its 768 KiB regression budget. This is a reproducible ordinary-SQL headroom
objective, not a guarantee that every asset stays below 1 MiB.

All byte values below are uncompressed. Source bytes count each SQL body once,
including authored/generated checks, helper definitions, and replacement scopes.

| Generic fixture | Source bytes | Baseline package ETF | Compact package ETF | Reduction | Baseline JSON | Compact JSON | Text nodes before / after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 ordinary columns | 435 | 38,960 | 3,104 | 92.0% | 23,492 | 2,490 | 181 / 2 |
| 100 ordinary columns | 4,037 | 361,886 | 13,910 | 96.2% | 219,924 | 13,582 | 1,711 / 2 |
| 1,000 ordinary columns | 41,839 | 3,672,656 | 127,340 | 96.5% | 2,250,078 | 129,704 | 17,011 / 2 |
| 5,000 ordinary columns | 217,839 | 18,480,656 | 655,340 | 96.5% | 11,545,124 | 669,712 | 85,011 / 2 |
| 100 columns, forty-space indentation | 7,637 | 1,106,624 | 24,710 | 97.8% | 668,066 | 24,382 | 5,311 / 2 |
| 100 columns with checks, nested helpers, scopes | 4,668 | 412,957 | 26,569 | 93.6% | 250,977 | 22,019 | 1,920 / 17 |
| 1,000 parameterized helper calls | 38,858 | 3,058,977 | 1,180,425 | 61.4% | 1,906,787 | 786,329 | 10,017 / 1,004 |

The baseline 5,000-column package exceeds publication/task limits; it is measured
offline to show representation cost, not presented as a publishable old package.

## The 1 MiB boundary

1 MiB is 1,048,576 bytes of the complete package's deterministic Erlang external
term format (ETF). An offline binary search brackets the first crossing for each
monotonically growing generic fixture; checks exercise fixed size budgets in CI.

| Fixture | Last package below 1 MiB | First package at or above 1 MiB |
| --- | --- | --- |
| Ordinary, baseline | 290 columns; 12,017 source bytes; 1,048,490 package bytes | 291 columns; 12,059 source bytes; 1,052,186 package bytes |
| Ordinary, compact | 7,979 columns; 348,915 source bytes; 1,048,568 package bytes | 7,980 columns; 348,959 source bytes; 1,048,700 package bytes |
| Helper/parameter-heavy, baseline | 346 calls; 13,350 source bytes; 1,048,574 package bytes | 347 calls; 13,389 source bytes; 1,051,648 package bytes |
| Helper/parameter-heavy, compact | 888 calls; 34,488 source bytes; 1,047,810 package bytes | 889 calls; 34,527 source bytes; 1,048,994 package bytes |

Each dynamic projection calls one helper with a distinct query parameter. Its
call, argument fragment, parameter, and diagnostic positions remain explicit.
Coalescing literal text cannot remove that cost. The 1 MiB objective is therefore
much harder to reach with ordinary literal SQL, while dynamic-heavy SQL can still
reach it with approximately 34 KiB of source. Further reduction there would need
a separately designed compact representation.

The 5,000-column task fixture is 656,622 raw bytes: 1,282 bytes above its package.
Its base64 payload is 875,496 bytes, before the small JSON persistence envelope.
The regression encodes and decodes its persistence envelope through the existing
codec below the old 1 MiB raw limit; it does not write a database row. This task deliberately has modest metadata; real pins,
parameters, target metadata, and other fields may consume more headroom.

## Codec and memory observations

The following observations use the 1,000-column ordinary fixture. Times are
medians of five warm samples; both implementations successfully verify their own
packages, including the full decoded JSON-to-package verification path.

| Operation | Baseline | Compact |
| --- | ---: | ---: |
| Canonical JSON encode | 201.0 ms | 1.1 ms |
| JSON decode to maps | 65.7 ms | 0.8 ms |
| ETF encode | 12.6 ms | 0.011 ms |
| ETF decode | 18.9 ms | 0.013 ms |
| Package verification | 1,361.8 ms | 5.3 ms |
| JSON decode, rehydrate, and verify | 2,499.9 ms | 11.3 ms |
| Retained process memory after GC | 6,665,064 bytes | 5,736 bytes |
| Referenced off-heap binary bytes at retained checkpoint | 83,934 bytes | 125,731 bytes |
| Sampled peak process memory through build and codec operations | 30,733,880 bytes | 8,238,248 bytes |
| Flat structural term words | 563,472 | 289 |

Timings vary with load, GC, runtime, and hardware; they are not latency promises.
Memory comes from isolated worker processes sampled every 2 ms. Process memory
excludes off-heap binaries, which are reported separately at the retained
checkpoint; flat words exclude those binaries too. Sampling can miss peaks, and
none of these values is operating-system RSS. Larger merged strings move storage
from many small heap objects into referenced binaries.

The experiment spans package construction and codec work. It does not isolate
parser peak memory: the unchanged lexer still constructs its token tree before
coalescing. It also does not measure database writes, dispatch, live deployment,
production SQL adapter execution, or constrained-container survival.

## Reproduce

Use the repository's normal dependencies and Elixir runtime. Run from the umbrella
root; these commands start no applications and touch no database. Normal Mix
compilation may update local build artifacts; the script writes the requested
report files. Baseline mode loads the historical parser and package
module in the measurement process, leaving the checkout and build files intact.

```bash
MIX_ENV=test mise exec -- mix run --no-start scripts/measure_sql_packages.exs --output work/sql-packages-after.json
MIX_ENV=test mise exec -- mix run --no-start scripts/measure_sql_packages.exs --baseline-ref 5f3c163e64f96c2b524c445b548f1177708c0bcd --output work/sql-packages-before.json
```

The baseline run can take several minutes because it repeatedly validates the
large original trees. It is a manual measurement, not a CI timing gate. Ordinary
CI runs `Favn.Manifest.SQLPackageSizeTest` with fixed byte and node budgets.
