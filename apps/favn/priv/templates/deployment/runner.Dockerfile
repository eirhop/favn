# syntax=docker/dockerfile:1.7

# Favn currently supports DuckDB SQL assets through ADBC only, so this generic
# runner includes the matching libduckdb driver and the extensions commonly
# required by that path. Keep these pins and checksums together when upgrading.
ARG DUCKDB_VERSION=1.5.5
ARG DUCKDB_LIB_SHA256=1fb8ce388157d84a25abe685a8a2520bf00c00321821968e4bb398fd766e7abb
ARG DUCKDB_CLI_SHA256=08c0ca117111fcede14239d0093792352befdc174218c344d232c13279643d05
ARG DUCKLAKE_EXTENSION_SHA256=733ccf19fedcfd5e0bfaf85993219145181099cc411076cabf14933ea16ab452
ARG POSTGRES_EXTENSION_SHA256=e0f631a5535f165468bc8a20501f8bc1490adbc877d38fcdff2f8d05531e1e5b
ARG JSON_EXTENSION_SHA256=325c0e08e081a928c66bba1528f3848e54dade9f82a8afe84f97df137333962e

FROM hexpm/elixir:1.20.2-erlang-29.0.4-debian-trixie-20260713-slim@sha256:5a4f24baf7f8963e4e03d0f63bf7d0e44babd1891ce9415b68e4c61846aab7b2 AS builder

ARG TARGETARCH
ARG DUCKDB_VERSION
ARG DUCKDB_LIB_SHA256
ARG DUCKDB_CLI_SHA256
ARG DUCKLAKE_EXTENSION_SHA256
ARG POSTGRES_EXTENSION_SHA256
ARG JSON_EXTENSION_SHA256

ENV MIX_ENV=prod
WORKDIR /build

# Debian packages come from a dated snapshot. Hex, Rebar, the base image, and
# every downloaded DuckDB artifact are also pinned for reproducible rebuilds.
RUN test "$TARGETARCH" = amd64 \
    && sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends binutils build-essential ca-certificates curl git gzip unzip \
    && rm -rf /var/lib/apt/lists/* \
    && mix local.hex 2.5.1 --force \
    && mix local.rebar rebar3 https://github.com/erlang/rebar3/releases/download/3.27.0/rebar3 \
         --sha512 0d00494d849fdc521a55142278d1f6ba552954fbd65b80d40df8022f594f05d6c99ed1d731bc263691a04176e11d4c6e126c56ba20dca19c5e42d4ffab2e7e36 \
         --force

# libduckdb is the ADBC driver. DuckLake uses postgres_scanner for PostgreSQL
# metadata catalogs, while JSON supports the standard JSON/NDJSON landing path.
# Remove an extension only after confirming the customer project does not
# execute SQL that uses it. Add cloud/object-store extensions here with a pinned
# checksum, offline LOAD verification, and a documented reason.
RUN set -eu; \
    duckdb_root="/opt/duckdb/$DUCKDB_VERSION"; \
    extension_root="/duckdb-home/.duckdb/extensions/v$DUCKDB_VERSION/linux_amd64"; \
    install -d -m 0755 "$duckdb_root" "$extension_root"; \
    curl --fail --location --silent --show-error \
      "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/libduckdb-linux-amd64.zip" \
      --output /tmp/libduckdb.zip; \
    curl --fail --location --silent --show-error \
      "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/duckdb_cli-linux-amd64.zip" \
      --output /tmp/duckdb-cli.zip; \
    curl --fail --location --silent --show-error \
      "https://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/ducklake.duckdb_extension.gz" \
      --output /tmp/ducklake.duckdb_extension.gz; \
    curl --fail --location --silent --show-error \
      "https://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/postgres_scanner.duckdb_extension.gz" \
      --output /tmp/postgres_scanner.duckdb_extension.gz; \
    curl --fail --location --silent --show-error \
      "https://extensions.duckdb.org/v$DUCKDB_VERSION/linux_amd64/json.duckdb_extension.gz" \
      --output /tmp/json.duckdb_extension.gz; \
    printf '%s  %s\n' \
      "$DUCKDB_LIB_SHA256" /tmp/libduckdb.zip \
      "$DUCKDB_CLI_SHA256" /tmp/duckdb-cli.zip \
      "$DUCKLAKE_EXTENSION_SHA256" /tmp/ducklake.duckdb_extension.gz \
      "$POSTGRES_EXTENSION_SHA256" /tmp/postgres_scanner.duckdb_extension.gz \
      "$JSON_EXTENSION_SHA256" /tmp/json.duckdb_extension.gz \
      | sha256sum --check --strict; \
    unzip -q /tmp/libduckdb.zip libduckdb.so -d "$duckdb_root"; \
    unzip -q /tmp/duckdb-cli.zip duckdb -d /usr/local/bin; \
    chmod 0755 /usr/local/bin/duckdb; \
    gzip -dc /tmp/ducklake.duckdb_extension.gz > /tmp/ducklake.duckdb_extension; \
    gzip -dc /tmp/postgres_scanner.duckdb_extension.gz > /tmp/postgres_scanner.duckdb_extension; \
    gzip -dc /tmp/json.duckdb_extension.gz > /tmp/json.duckdb_extension; \
    nm -D --defined-only "$duckdb_root/libduckdb.so" | grep -F 'duckdb_adbc_init' >/dev/null; \
    HOME=/duckdb-home duckdb :memory: -batch -c \
      "INSTALL '/tmp/ducklake.duckdb_extension'; INSTALL '/tmp/postgres_scanner.duckdb_extension'; INSTALL '/tmp/json.duckdb_extension';"; \
    HOME=/duckdb-home duckdb :memory: -batch -c \
      "SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; SET allow_community_extensions = false; LOAD ducklake; LOAD postgres; LOAD json; SELECT json_valid('{}');"; \
    rm -f /tmp/libduckdb.zip /tmp/duckdb-cli.zip /tmp/*.duckdb_extension /tmp/*.duckdb_extension.gz

ARG FAVN_CUSTOMER_APP
ARG FAVN_PROJECT_ROOT=.
ENV FAVN_CUSTOMER_APP=$FAVN_CUSTOMER_APP

# The customer source is mounted only for this build step; it is not retained in
# a builder layer. The release cookie is removed before the runtime copy.
WORKDIR /build/${FAVN_PROJECT_ROOT}/deploy/favn
RUN --mount=type=bind,source=.,target=/build,rw \
    --mount=type=cache,target=/build/${FAVN_PROJECT_ROOT}/deps \
    --mount=type=cache,target=/build/${FAVN_PROJECT_ROOT}/_build \
    --mount=type=cache,target=/root/.cache/elixir_make \
    set -eu; \
    test -n "$FAVN_CUSTOMER_APP"; \
    mix deps.get --only prod --check-locked; \
    mix deps.compile; \
    mix release favn_runner --path /runner-release; \
    rm -f /runner-release/releases/COOKIE; \
    test ! -e /runner-release/releases/COOKIE

FROM debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS runtime

ARG TARGETARCH
ARG DUCKDB_VERSION
ARG FAVN_RUNNER_RELEASE_ID
ARG FAVN_RUNNER_SOURCE=unknown
ARG FAVN_SOURCE_REVISION=unknown
ARG FAVN_BUILD_TIMESTAMP=unknown

# A release ID binds the image to manifests; it is deliberately not inferred
# from a mutable tag. The runtime owns only its home and /tmp. Application code,
# the driver, and preinstalled extensions remain root-owned and immutable.
RUN test "$TARGETARCH" = amd64 \
    && case "$FAVN_RUNNER_RELEASE_ID" in \
      rr_*) ;; \
      *) echo "FAVN_RUNNER_RELEASE_ID must be rr_ plus 64 lowercase hex characters" >&2; exit 1 ;; \
    esac \
    && release_hex="${FAVN_RUNNER_RELEASE_ID#rr_}" \
    && [ "${#release_hex}" -eq 64 ] \
    && case "$release_hex" in \
      *[!0-9a-f]*) echo "FAVN_RUNNER_RELEASE_ID must be rr_ plus 64 lowercase hex characters" >&2; exit 1 ;; \
      *) ;; \
    esac \
    && sed -i \
      -e 's|URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z|' \
      -e 's|URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z|' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 favn \
    && useradd --uid 10001 --gid 10001 --create-home --home-dir /var/lib/favn --shell /usr/sbin/nologin --no-log-init favn \
    && chmod 0700 /var/lib/favn \
    && find / -xdev -type f -perm /6000 -exec chmod a-s {} +

WORKDIR /opt/favn
COPY --link --from=builder /runner-release/ /opt/favn/
COPY --link --from=builder /opt/duckdb/ /opt/duckdb/
COPY --link --from=builder /duckdb-home/.duckdb/ /var/lib/favn/.duckdb/

LABEL org.opencontainers.image.title="Favn customer runner" \
      org.opencontainers.image.description="Customer-owned Favn runner with the supported DuckDB ADBC runtime" \
      org.opencontainers.image.source="$FAVN_RUNNER_SOURCE" \
      org.opencontainers.image.revision="$FAVN_SOURCE_REVISION" \
      org.opencontainers.image.created="$FAVN_BUILD_TIMESTAMP" \
      org.opencontainers.image.version="$FAVN_RUNNER_RELEASE_ID" \
      io.favn.runner-release-id="$FAVN_RUNNER_RELEASE_ID" \
      io.favn.duckdb-version="$DUCKDB_VERSION" \
      io.favn.elixir-version="1.20.2" \
      io.favn.otp-version="29.0.4" \
      io.favn.target="linux/amd64"

ENV FAVN_RUNNER_RELEASE_ID=$FAVN_RUNNER_RELEASE_ID \
    DUCKDB_ADBC_DRIVER=/opt/duckdb/${DUCKDB_VERSION}/libduckdb.so \
    HOME=/var/lib/favn \
    ERL_CRASH_DUMP=/tmp/erl_crash.dump \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

USER 10001:10001
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=20 CMD ["/opt/favn/bin/favn_runner", "rpc", "case FavnRunner.readiness() do :ok -> :ok; other -> raise inspect(other) end"]
ENTRYPOINT ["/opt/favn/bin/favn_runner"]
CMD ["start"]
