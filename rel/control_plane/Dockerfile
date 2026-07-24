FROM --platform=linux/amd64 hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260713-slim@sha256:6fcd8ea864221b960c1ec418e3b10fa488298ff9e70c9e0f3db18070e610fb8a AS builder

ENV MIX_ENV=prod
WORKDIR /build

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
RUN test "$(cat /usr/local/lib/erlang/releases/29/OTP_VERSION)" = 29.0.3 \
    && install -d /build/runtime-versions \
    && cp /usr/local/lib/erlang/releases/29/OTP_VERSION /build/runtime-versions/OTP_VERSION \
    && elixir -e 'IO.write(System.version())' > /build/runtime-versions/ELIXIR_VERSION \
    && test "$(cat /build/runtime-versions/ELIXIR_VERSION)" = 1.20.2

COPY . .
RUN mix deps.get --only prod --check-locked && mix deps.compile
RUN mix do --app favn_view cmd mix tailwind.install \
    && echo 'dc61b3ac6b8c9ca874c0cc4c57b2409791a64c5540404ca5f5367360babc313a  /build/_build/tailwind-linux-x64-4.3.3' | sha256sum --check --strict
RUN mix do --app favn_view cmd mix esbuild.install \
    && echo '0c6588b092a2c291a72bab90659f3c9e0e25e0fe59c9ac12b4dae4d945e5548c  /build/_build/esbuild-linux-x64' | sha256sum --check --strict

RUN mix do --app favn_view cmd mix assets.deploy
RUN mix release favn_control_plane --overwrite \
    && elixir rel/control_plane/sanitize_sys_config.exs _build/prod/rel/favn_control_plane

FROM --platform=linux/amd64 debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS runtime

RUN sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 favn \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /tmp/favn --shell /usr/sbin/nologin favn \
    && install -d -o 10001 -g 10001 -m 0700 /tmp/favn

WORKDIR /app
COPY --from=builder --chown=10001:10001 /build/_build/prod/rel/favn_control_plane/ /app/
COPY --from=builder --chown=10001:10001 /build/runtime-versions/ /app/runtime-versions/
RUN rm -f /app/releases/COOKIE \
    && find /app -type f -name '*.map' -delete \
    && for templates in /app/lib/phoenix-*/priv/templates; do \
         if [ -e "$templates" ]; then rm -rf "$templates"; fi; \
       done \
    && chmod 0555 /app/bin/favn_control_plane_health /app/bin/favn_control_plane_ops

ARG FAVN_SOURCE_REVISION=unknown
ARG FAVN_BUILD_TIMESTAMP=unknown
ARG FAVN_CONTROL_PLANE_VERSION=0.5.0-dev
ARG FAVN_MANIFEST_SCHEMA_VERSION=12
ARG FAVN_RUNNER_CONTRACT_VERSION=12

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
      io.favn.otp-version="29.0.3" \
      io.favn.target="linux/amd64"

ENV HOME=/tmp/favn \
    RELEASE_TMP=/tmp/favn \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    FAVN_VIEW_PORT=4000

EXPOSE 4000 4101
USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=6 CMD ["/app/bin/favn_control_plane_health"]
ENTRYPOINT ["/app/bin/favn_control_plane"]
CMD ["start"]
