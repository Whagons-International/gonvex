# File-level backups for the runtime's SQLite data directory.
# The runtime owns every database file; this sidecar only reads them through
# SQLite's online backup API, which copies a consistent snapshot of one file
# while the runtime keeps writing (WAL mode).
FROM alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
RUN apk add --no-cache sqlite
COPY backup.sh /usr/local/bin/gonvex-sqlite-backup
RUN chmod 0755 /usr/local/bin/gonvex-sqlite-backup
ENTRYPOINT ["/usr/local/bin/gonvex-sqlite-backup"]
CMD ["loop"]
