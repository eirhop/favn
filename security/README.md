# Image vulnerability exceptions

The [Grype policy](control-plane-grype.yaml) contains reviewed exceptions for
the shipped Debian 13 images. The High severity gate, scanning of unfixed
findings, and machine-checked review deadline remain enforced. An exception is
an applicability or residual-risk assessment, not a package patch or a claim
that privileged/customized deployments are safe.

## Temporary monetary-formatting exception, 15 September 2026

[CVE-2026-19499](https://security-tracker.debian.org/tracker/CVE-2026-19499)
affects glibc's `strfmon` and `strfmon_l` monetary formatting when right-justified
padding writes past a caller-supplied buffer. Debian marks trixie `no-dsa`
(minor issue); its `glibc 2.41-12+deb13u4` remains vulnerable with no trixie
package fix. The advisory reports no known network-facing application impact;
that is not a guarantee that every application is unaffected.

Grype 0.116.0 with database built `2026-09-15T06:31:36Z` reports two High
matches, `libc6` and `libc-bin`, in the previously qualified runtime image.
The temporary rules require this CVE, Debian 13, package type `deb`, the exact
installed version `2.41-12+deb13u4`, and fix state `wont-fix`. A package version
or vendor fix-state change stops the rules matching. The High severity gate
and scanning of unfixed findings remain enabled.

The applicability assessment checks the shipped ELF binaries for imports of
both affected functions and literal symbol names used for dynamic lookup.
The control-plane audit examined 701 ELF files from the previously qualified
image at source revision `380a7364`; the generic runner audit examined 684 ELF
files built from revision `0b0c8bfa`, including DuckDB and its shipped extensions.
Both found no affected imports, symbol names only in libc itself, and no ELF
inspection errors. This PR changes no native runtime dependencies. Independent
review and scan results are recorded in [PR #711](https://github.com/eirhop/favn/pull/711). Favn source contains no calls to
these functions. This supports a temporary assessment of the shipped binaries,
not a proof that arbitrary native code cannot reach the vulnerable functions.
Additional native plugins, DuckDB extensions, or dynamically loaded libraries
require reassessment; do not rely on this exception for customized native code.
The vulnerable libc functions remain present and unpatched.

The policy has one shared deadline. It is brought forward from 1 October to
**28 September 2026**, so `scripts/check_grype_exceptions.sh` rejects builds
starting **29 September 2026**. This enforces the two-week limit without adding
a separate expiry mechanism and also advances review of the existing rules.
Remove these two rules when a fix is available, or reassess and explicitly
review them before the deadline; do not automatically extend them.

## Runtime package refresh of 14 September 2026

The runtime stages of both shipped images now use Debian and Debian-security
snapshot `20260914T000000Z`. This replaces the August snapshot, which predates
vendor patches for 20 High/Critical package matches across 18 advisories reported
by Grype 0.116.0 (database built `2026-09-14T06:38:38Z`). The initial reproduction
used the runtime package set; full image scans remain the release gate.

| Package | Patched version required by both image contracts | Vendor evidence |
| --- | --- | --- |
| libc6, libc-bin | `2.41-12+deb13u4` | [glibc](https://security-tracker.debian.org/tracker/CVE-2026-5450) |
| perl-base | `5.40.1-6+deb13u1` | [Perl](https://security-tracker.debian.org/tracker/CVE-2026-13221) |
| gzip | `1.13-1+deb13u1` | [gzip](https://security-tracker.debian.org/tracker/CVE-2026-41992) |
| libpcre2-8-0 | `10.46-1~deb13u2` | [PCRE2](https://security-tracker.debian.org/tracker/CVE-2026-86145) |
| libsqlite3-0 | `3.46.1-7+deb13u2` | [SQLite](https://security-tracker.debian.org/tracker/CVE-2026-11822) |

Eighteen obsolete package-specific exception rules were removed for the fixed
glibc, Perl, gzip and SQLite findings. The two PCRE2 findings had no exception.
That refresh kept the High severity gate, scanning of unfixed findings, remaining
applicability constraints and then-current 1 October review deadline unchanged. Image
contracts compare installed versions using Debian version ordering and also accept later patches.

The complete runner scan additionally found 11 High matches in bundled Erlang
29.0.4. Both builders now use [OTP 29.0.6](https://github.com/erlang/otp/releases/tag/OTP-29.0.6)
and [Elixir 1.20.4](https://github.com/elixir-lang/elixir/releases/tag/v1.20.4),
which also includes a security patch. The published Hex image pairs those patch
versions with Debian trixie `20260824`; its index is pinned to
`sha256:3eade7c27e7e3022842799ae0933b69ce29005e556d570c001b18bce93ebd325`.
Builder package archives match that date. CI and the Compose customer builder
use the same toolchain pair. Image contracts execute each bundled release and
assert Elixir 1.20.4 and ERTS 17.0.6, in addition to image metadata checks.

The runtime Debian base digest and application dependencies are unchanged. The
retained assessments below describe the exact versions and conditions originally
reviewed; a package or vendor fix-state change does not broaden an exception.

## Review of 4 September 2026

Grype 0.116.0 with its database built at `2026-09-04T06:30:46Z` reports 37 new
High matches in the runtime package setup: four util-linux advisories across
nine binary packages and one zlib advisory. Debian has no stable-release fix for
these findings at this review. That review retained the deadline of
**1 October 2026**; it was not extended.

| Advisory | Assessment |
| --- | --- |
| [CVE-2026-76642](https://security-tracker.debian.org/tracker/CVE-2026-76642) | Privileged mount hooks after a failed helper; Debian marks trixie no-dsa, reported by Grype as `wont-fix`. |
| [CVE-2026-78409](https://security-tracker.debian.org/tracker/CVE-2026-78409) | Privileged `X-mount.subdir` path resolution; same Debian disposition. |
| [CVE-2026-78410](https://security-tracker.debian.org/tracker/CVE-2026-78410) | Privileged bind-mount source replacement; same Debian disposition. |
| [CVE-2026-78408](https://security-tracker.debian.org/tracker/CVE-2026-78408) | A privileged `nsenter --join-cgroup` caller leaks root-opened descriptors; same Debian disposition, with the separate host/operator restriction below. |
| [CVE-2026-85091](https://security-tracker.debian.org/tracker/CVE-2026-85091) | Debian still reports zlib as unresolved/unfixed; the exact installed 1.3.1 source lacks the later affected function. |

### util-linux deployment conditions

Both shipped image contracts require UID/GID 10001, no SUID/SGID executables,
and successful execution with all Linux capabilities dropped. The mount
exceptions apply to that qualified deployment scope, which excludes privileged
mount operations. Root execution, extra capabilities, SUID restoration, and
custom privileged helpers require a new assessment.

The nsenter finding has a distinct prerequisite. As the
[upstream advisory](https://github.com/util-linux/util-linux/security/advisories/GHSA-55fx-f4gg-cfhj)
explains, a capability-free, non-root target can exploit a root-opened cgroup
descriptor inherited from a privileged operator's `nsenter --join-cgroup`.
Do not use that affected host/operator operation or inject privileged cgroup
descriptors into these runtimes. Favn launchers do not invoke it. Image UID and
capability controls alone do not prevent this scenario, and image qualification
cannot establish host safety.

Each util-linux rule names one advisory and requires the Debian 13 namespace,
package type `deb`, fix state `wont-fix`, and a closed, anchored list of the nine
observed binary package names. It stops matching if Debian reports a fix; it
does not suppress that advisory for other distributions or package names.

### zlib source evidence

The installed binary package is `zlib1g 1:1.3.dfsg+really1.3.1-1+b1`. Its source
is `zlib 1:1.3.dfsg+really1.3.1-1`, retrieved with authenticated APT source
metadata from the same 26 August 2026 snapshot as the runtime Dockerfiles.

The source archive's `gzwrite.c` is byte-for-byte equal to
[upstream v1.3.1](https://github.com/madler/zlib/blob/v1.3.1/gzwrite.c).
It contains no `gz_vacate`; the Debian patch series is empty. The advisory names
that later function, also shown in the
[upstream change linked by Debian](https://github.com/madler/zlib/commit/e3dc0a85b7032e98380dec011bc8f2c2ee0d8fca).
This supports a version-specific non-applicability assessment while Debian's
tracker remains unresolved; it does not imply every zlib version is unaffected.

Authenticated archive SHA-256 values:

- `zlib_1.3.dfsg+really1.3.1.orig.tar.gz`:
  `60dd315c07f616887caa029408308a018ace66e3d142726a97db164b3b8f69fb`
- `zlib_1.3.dfsg+really1.3.1-1.debian.tar.xz`:
  `9ed525955ce9fb0c1b39be8ff98f73450dbfc6305a9a27e6149c8972d38a0a9e`

The rule requires the exact binary version above, Debian 13, package type `deb`,
and fix state `not-fixed`. A version or fix-state change requires reassessment.
