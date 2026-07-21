FROM --platform=linux/amd64 hexpm/elixir:1.20.2-erlang-28.3.3-debian-bookworm-20260713-slim@sha256:874b36d3e432c42a4f78e12fbe251c5e6c3b1342c8f1072e25dc418b823c31ba AS builder

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
    && mix local.rebar rebar3 https://builds.hex.pm/installs/1.18.4/rebar3-3.25.1-otp-28 \
         --sha512 992fd755b7926fae455e5e07d9d195f4d3e7f181609eed1b9cabfe548624df10d148cd4b59bda40bebb185d3d68f9a9fd68a70b294101c8ad9cf0fadcc683d24 \
         --force
RUN test "$(cat /usr/local/lib/erlang/releases/28/OTP_VERSION)" = 28.3.3 \
    && install -d /build/runtime-versions \
    && cp /usr/local/lib/erlang/releases/28/OTP_VERSION /build/runtime-versions/OTP_VERSION \
    && elixir -e 'IO.write(System.version())' > /build/runtime-versions/ELIXIR_VERSION \
    && test "$(cat /build/runtime-versions/ELIXIR_VERSION)" = 1.20.2

COPY mix.exs mix.lock ./
COPY config/ config/
COPY rel/control_plane/ rel/control_plane/
COPY apps/favn_core/mix.exs apps/favn_core/mix.exs
COPY apps/favn_orchestrator/mix.exs apps/favn_orchestrator/mix.exs
COPY apps/favn_storage_postgres/mix.exs apps/favn_storage_postgres/mix.exs
COPY apps/favn_view/mix.exs apps/favn_view/mix.exs
RUN mix deps.get --only prod --check-locked && mix deps.compile
RUN mix do --app favn_view cmd mix tailwind.install \
    && echo '5eeee66ea237eae9a160fa3314fd0cf76ab993551a99fafb16fa1db6c6b90289  /build/_build/tailwind-linux-x64-4.1.12' | sha256sum --check --strict
RUN mix do --app favn_view cmd mix esbuild.install \
    && echo '93433b456cac3a454ee27403d3de9adce88d83e5439ba37e1471af54730c9ca7  /build/_build/esbuild-linux-x64' | sha256sum --check --strict

COPY apps/ apps/
COPY control-plane-build.json control-plane-build.json
RUN mix do --app favn_view cmd mix assets.deploy
RUN mix release favn_control_plane --overwrite \
    && elixir rel/control_plane/sanitize_sys_config.exs _build/prod/rel/favn_control_plane

FROM --platform=linux/amd64 debian:bookworm-slim@sha256:63a496b5d3b99214b39f5ed70eb71a61e590a77979c79cbee4faf991f8c0783e AS runtime

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
COPY --from=builder --chown=10001:10001 /build/control-plane-build.json /app/control-plane-build.json
COPY --from=builder --chown=10001:10001 /build/runtime-versions/ /app/runtime-versions/
RUN rm -f /app/releases/COOKIE \
    && find /app -type f -name '*.map' -delete \
    && for templates in /app/lib/phoenix-*/priv/templates; do \
         if [ -e "$templates" ]; then rm -rf "$templates"; fi; \
       done \
    && chmod 0555 /app/bin/favn_control_plane_health /app/bin/favn_control_plane_ops

ARG FAVN_CONTROL_PLANE_BUILD_ID
ARG FAVN_SOURCE_REVISION=unknown
ARG FAVN_BUILD_TIMESTAMP=unknown
ARG FAVN_CONTROL_PLANE_VERSION
ARG FAVN_MANIFEST_SCHEMA_VERSION
ARG FAVN_RUNNER_CONTRACT_VERSION

LABEL org.opencontainers.image.title="Favn control plane" \
      org.opencontainers.image.description="Favn View and Orchestrator production control plane" \
      org.opencontainers.image.source="https://github.com/eirhop/favn" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="$FAVN_SOURCE_REVISION" \
      org.opencontainers.image.created="$FAVN_BUILD_TIMESTAMP" \
      org.opencontainers.image.version="$FAVN_CONTROL_PLANE_VERSION" \
      io.favn.control-plane.build-id="$FAVN_CONTROL_PLANE_BUILD_ID" \
      io.favn.manifest-schema-version="$FAVN_MANIFEST_SCHEMA_VERSION" \
      io.favn.runner-contract-version="$FAVN_RUNNER_CONTRACT_VERSION" \
      io.favn.elixir-version="1.20.2" \
      io.favn.otp-version="28.3.3" \
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
