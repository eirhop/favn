# syntax=docker/dockerfile:1.7

# These values have no defaults on purpose. CI derives them from the checked-out
# source, and the builder verifies them before they can become OCI labels.
ARG FAVN_CONTROL_PLANE_VERSION
ARG FAVN_MANIFEST_SCHEMA_VERSION
ARG FAVN_RUNNER_CONTRACT_VERSION

FROM hexpm/elixir:1.20.2-erlang-29.0.4-debian-trixie-20260713-slim@sha256:5a4f24baf7f8963e4e03d0f63bf7d0e44babd1891ce9415b68e4c61846aab7b2 AS builder

ARG TARGETARCH
ARG FAVN_CONTROL_PLANE_VERSION
ARG FAVN_MANIFEST_SCHEMA_VERSION
ARG FAVN_RUNNER_CONTRACT_VERSION

ENV MIX_ENV=prod
WORKDIR /build

RUN test "$TARGETARCH" = amd64 \
    && test -n "$FAVN_CONTROL_PLANE_VERSION" \
    && test -n "$FAVN_MANIFEST_SCHEMA_VERSION" \
    && test -n "$FAVN_RUNNER_CONTRACT_VERSION"

RUN sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN mix local.hex 2.5.1 --force \
    && mix local.rebar rebar3 https://github.com/erlang/rebar3/releases/download/3.27.0/rebar3 \
         --sha512 0d00494d849fdc521a55142278d1f6ba552954fbd65b80d40df8022f594f05d6c99ed1d731bc263691a04176e11d4c6e126c56ba20dca19c5e42d4ffab2e7e36 \
         --force

# The source is bind-mounted instead of copied into an image layer. BuildKit
# caches dependencies and compiled output, while the release itself is written
# to a clean path that is copied into the runtime stage.
RUN --mount=type=bind,source=.,target=/build,rw \
    --mount=type=cache,target=/build/deps \
    --mount=type=cache,target=/build/_build \
    set -eu; \
    test "$(cat /usr/local/lib/erlang/releases/29/OTP_VERSION)" = 29.0.4; \
    test "$(elixir -e 'IO.write(System.version())')" = 1.20.2; \
    mix deps.get --only prod --check-locked; \
    mix deps.compile; \
    mix do --app favn_view cmd mix tailwind.install; \
    echo 'dc61b3ac6b8c9ca874c0cc4c57b2409791a64c5540404ca5f5367360babc313a  /build/_build/tailwind-linux-x64-4.3.3' | sha256sum --check --strict; \
    for attempt in 1 2 3; do \
      mix do --app favn_view cmd mix esbuild.install && break; \
      if [ "$attempt" -eq 3 ]; then exit 1; fi; \
      sleep $((attempt * 5)); \
    done; \
    echo '0c6588b092a2c291a72bab90659f3c9e0e25e0fe59c9ac12b4dae4d945e5548c  /build/_build/esbuild-linux-x64' | sha256sum --check --strict; \
    mix do --app favn_view cmd mix assets.deploy; \
    install -d -m 0755 /runtime-versions; \
    mix run --no-start -e 'File.write!("/runtime-versions/FAVN_VERSION", Mix.Project.config()[:version]); File.write!("/runtime-versions/MANIFEST_SCHEMA_VERSION", Integer.to_string(Favn.Manifest.ContractVersions.manifest_schema_version())); File.write!("/runtime-versions/RUNNER_CONTRACT_VERSION", Integer.to_string(Favn.Manifest.ContractVersions.runner_contract_version()))'; \
    test "$(cat /runtime-versions/FAVN_VERSION)" = "$FAVN_CONTROL_PLANE_VERSION"; \
    test "$(cat /runtime-versions/MANIFEST_SCHEMA_VERSION)" = "$FAVN_MANIFEST_SCHEMA_VERSION"; \
    test "$(cat /runtime-versions/RUNNER_CONTRACT_VERSION)" = "$FAVN_RUNNER_CONTRACT_VERSION"; \
    mix release favn_control_plane --overwrite --path /control-plane-release; \
    test -d "/control-plane-release/releases/$FAVN_CONTROL_PLANE_VERSION"; \
    elixir rel/control_plane/sanitize_sys_config.exs /control-plane-release; \
    rm -f /control-plane-release/releases/COOKIE; \
    find /control-plane-release -type f -name '*.map' -delete; \
    for templates in /control-plane-release/lib/phoenix-*/priv/templates; do \
      if [ -e "$templates" ]; then rm -rf "$templates"; fi; \
    done; \
    chmod 0555 /control-plane-release/bin/favn_control_plane_health /control-plane-release/bin/favn_control_plane_ops; \
    cp /usr/local/lib/erlang/releases/29/OTP_VERSION /runtime-versions/OTP_VERSION; \
    elixir -e 'IO.write(System.version())' > /runtime-versions/ELIXIR_VERSION; \
    test ! -e /control-plane-release/releases/COOKIE

FROM debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS runtime

ARG TARGETARCH
ARG FAVN_SOURCE_REVISION=unknown
ARG FAVN_BUILD_TIMESTAMP=unknown
ARG FAVN_CONTROL_PLANE_VERSION
ARG FAVN_MANIFEST_SCHEMA_VERSION
ARG FAVN_RUNNER_CONTRACT_VERSION

RUN test "$TARGETARCH" = amd64 \
    && test -n "$FAVN_CONTROL_PLANE_VERSION" \
    && test -n "$FAVN_MANIFEST_SCHEMA_VERSION" \
    && test -n "$FAVN_RUNNER_CONTRACT_VERSION" \
    && sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 favn \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /tmp --shell /usr/sbin/nologin --no-log-init favn \
    && find / -xdev -type f -perm /6000 -exec chmod a-s {} +

WORKDIR /app
COPY --link --from=builder /control-plane-release/ /app/
COPY --link --from=builder /runtime-versions/ /app/runtime-versions/

LABEL org.opencontainers.image.title="Favn control plane" \
      org.opencontainers.image.description="Favn View and Orchestrator production control plane" \
      org.opencontainers.image.source="https://github.com/eirhop/favn" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="$FAVN_SOURCE_REVISION" \
      org.opencontainers.image.created="$FAVN_BUILD_TIMESTAMP" \
      org.opencontainers.image.version="$FAVN_CONTROL_PLANE_VERSION" \
      io.favn.manifest-schema-version="$FAVN_MANIFEST_SCHEMA_VERSION" \
      io.favn.runner-contract-version="$FAVN_RUNNER_CONTRACT_VERSION" \
      io.favn.elixir-version="1.20.2" \
      io.favn.otp-version="29.0.4" \
      io.favn.target="linux/amd64"

ENV HOME=/tmp \
    RELEASE_TMP=/tmp \
    ERL_CRASH_DUMP=/tmp/erl_crash.dump \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    FAVN_VIEW_PORT=4000

EXPOSE 4000 4101
USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=6 CMD ["/app/bin/favn_control_plane_health"]
ENTRYPOINT ["/app/bin/favn_control_plane"]
CMD ["start"]
