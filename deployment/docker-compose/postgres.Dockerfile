FROM postgres@sha256:1961f96e6029a02c3812d7cb329a3b03a3ac2bb067058dec17b0f5596aca9296

COPY postgres-entrypoint.sh /usr/local/bin/favn-postgres-entrypoint
RUN chmod 0555 /usr/local/bin/favn-postgres-entrypoint

ENTRYPOINT ["/usr/local/bin/favn-postgres-entrypoint"]
CMD ["postgres"]
