FROM docker:29-cli

RUN apk add --no-cache curl jq

COPY scale-runners.sh /usr/local/bin/scale-favn-runners
RUN chmod 0555 /usr/local/bin/scale-favn-runners

ENTRYPOINT ["/usr/local/bin/scale-favn-runners"]
