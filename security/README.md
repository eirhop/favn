# Image vulnerability exceptions

The [Grype policy](control-plane-grype.yaml) contains reviewed exceptions for
the shipped Debian 13 images. The High severity gate, scanning of unfixed
findings, and machine-checked review deadline remain enforced. An exception is
an applicability or residual-risk assessment, not a package patch or a claim
that privileged/customized deployments are safe.

## Review of 4 September 2026

Grype 0.116.0 with its database built at `2026-09-04T06:30:46Z` reports 37 new
High matches in the runtime package setup: four util-linux advisories across
nine binary packages and one zlib advisory. Debian has no stable-release fix for
these findings at this review. The existing review deadline remains
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
