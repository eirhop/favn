# syntax=docker/dockerfile:1.7

FROM hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260713-slim AS builder

ENV MIX_ENV=prod
WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && mix local.hex --force \
    && mix local.rebar --force

COPY . .

ARG FAVN_CUSTOMER_APP
ARG FAVN_PROJECT_ROOT=.
ENV FAVN_CUSTOMER_APP=$FAVN_CUSTOMER_APP

WORKDIR /build/${FAVN_PROJECT_ROOT}/deploy/favn
RUN mix deps.get --only prod --check-locked \
    && mix deps.compile \
    && mix release favn_runner --path /runner-release

FROM debian:trixie-slim AS runtime

ARG FAVN_RUNNER_RELEASE_ID
RUN case "$FAVN_RUNNER_RELEASE_ID" in \
      rr_*) ;; \
      *) echo "FAVN_RUNNER_RELEASE_ID must be rr_ plus 64 lowercase hex characters" >&2; exit 1 ;; \
    esac \
    && release_hex="${FAVN_RUNNER_RELEASE_ID#rr_}" \
    && [ "${#release_hex}" -eq 64 ] \
    && case "$release_hex" in \
      *[!0-9a-f]*) echo "FAVN_RUNNER_RELEASE_ID must be rr_ plus 64 lowercase hex characters" >&2; exit 1 ;; \
      *) ;; \
    esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgcc-s1 openssl libncurses6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 favn \
    && useradd --system --uid 10001 --gid favn --home-dir /var/lib/favn --shell /usr/sbin/nologin favn

WORKDIR /opt/favn
COPY --from=builder --chown=10001:10001 /runner-release/ ./
RUN rm -f /opt/favn/releases/COOKIE

ENV FAVN_RUNNER_RELEASE_ID=$FAVN_RUNNER_RELEASE_ID \
    HOME=/var/lib/favn \
    LANG=C.UTF-8

LABEL io.favn.runner-release-id="$FAVN_RUNNER_RELEASE_ID"

USER 10001:10001
EXPOSE 4369 9100
ENTRYPOINT ["/opt/favn/bin/favn_runner"]
CMD ["start"]
