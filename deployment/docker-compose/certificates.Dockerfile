FROM alpine:3.22

RUN apk add --no-cache openssl

COPY generate-certificates.sh /usr/local/bin/generate-certificates
RUN chmod 0555 /usr/local/bin/generate-certificates

ENTRYPOINT ["/usr/local/bin/generate-certificates"]
