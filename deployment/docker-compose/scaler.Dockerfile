FROM docker:29-cli

RUN apk add --no-cache curl jq postgresql17-client

COPY scale-runners.sh /usr/local/bin/scale-favn-runners
COPY qualify-postgres.sh /usr/local/bin/qualify-favn-postgres
COPY qualification-observe.sql /usr/local/share/favn/qualification-observe.sql
COPY qualification-outcomes.sql /usr/local/share/favn/qualification-outcomes.sql
RUN chmod 0555 \
  /usr/local/bin/scale-favn-runners \
  /usr/local/bin/qualify-favn-postgres

ENTRYPOINT ["/usr/local/bin/scale-favn-runners"]
