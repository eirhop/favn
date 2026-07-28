# syntax=docker/dockerfile:1.7

FROM hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260713-slim AS customer-builder

ENV MIX_ENV=prod
WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && mix local.hex 2.5.1 --force \
    && mix local.rebar rebar3 https://github.com/erlang/rebar3/releases/download/3.27.0/rebar3 \
         --sha512 0d00494d849fdc521a55142278d1f6ba552954fbd65b80d40df8022f594f05d6c99ed1d731bc263691a04176e11d4c6e126c56ba20dca19c5e42d4ffab2e7e36 \
         --force

COPY . .

ARG FAVN_RUNNER_RELEASE_ID
RUN case "$FAVN_RUNNER_RELEASE_ID" in \
      rr_????????????????????????????????????????????????????????????????) ;; \
      *) echo "invalid FAVN_RUNNER_RELEASE_ID" >&2; exit 1 ;; \
    esac

WORKDIR /build/examples/basic-workflow-tutorial
RUN mix deps.get --only prod --check-locked \
    && mix deps.compile \
    && mix compile --warnings-as-errors \
    && mix favn.build.manifest --runner-release "default=$FAVN_RUNNER_RELEASE_ID"

FROM customer-builder AS operator

COPY deployment/docker-compose/operator.sh /usr/local/bin/favn-simulation-operator
RUN chmod 0555 /usr/local/bin/favn-simulation-operator

WORKDIR /build/examples/basic-workflow-tutorial
ENTRYPOINT ["/usr/local/bin/favn-simulation-operator"]
CMD ["help"]

FROM customer-builder AS runner-builder

ENV FAVN_CUSTOMER_APP=crm_demo
WORKDIR /build/deployment/docker-compose/runner-release
RUN mix deps.get --only prod --check-locked \
    && mix release favn_runner --path /runner-release

FROM debian:trixie-slim AS runner

ARG FAVN_RUNNER_RELEASE_ID
RUN case "$FAVN_RUNNER_RELEASE_ID" in \
      rr_*) ;; \
      *) echo "FAVN_RUNNER_RELEASE_ID must start with rr_" >&2; exit 1 ;; \
    esac \
    && release_hex="${FAVN_RUNNER_RELEASE_ID#rr_}" \
    && [ "${#release_hex}" -eq 64 ] \
    && case "$release_hex" in \
      *[!0-9a-f]*) echo "FAVN_RUNNER_RELEASE_ID must contain lowercase hex" >&2; exit 1 ;; \
      *) ;; \
    esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 openssl libncurses6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 favn \
    && useradd --system --uid 10001 --gid favn --home-dir /var/lib/favn --shell /usr/sbin/nologin favn

WORKDIR /opt/favn
COPY --from=runner-builder --chown=10001:10001 /runner-release/ ./
RUN rm -f /opt/favn/releases/COOKIE

ENV FAVN_RUNNER_RELEASE_ID=$FAVN_RUNNER_RELEASE_ID \
    HOME=/var/lib/favn \
    LANG=C.UTF-8

LABEL io.favn.runner-release-id="$FAVN_RUNNER_RELEASE_ID"

USER 10001:10001
ENTRYPOINT ["/opt/favn/bin/favn_runner"]
CMD ["start"]
