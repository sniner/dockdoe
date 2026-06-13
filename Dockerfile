# syntax=docker/dockerfile:1

# --- Build: fully static musl binary -----------------------------------------
FROM rust:1-alpine AS builder

# build-base pulls in gcc/make for the bundled SQLite (the cc crate compiles
# sqlite3.c); musl headers come with it. cmake is for aws-lc-rs, the rustls
# crypto provider behind reqwest's HTTPS (Apprise notifications). TLS uses
# rustls, not OpenSSL, so the binary stays fully static.
RUN apk add --no-cache build-base cmake

WORKDIR /src
COPY . .

# The alpine toolchain targets musl with crt-static by default → static binary.
RUN cargo build --release --target x86_64-unknown-linux-musl \
    && strip target/x86_64-unknown-linux-musl/release/dockdoe \
    && mkdir -p /out/data

# --- Runtime: scratch (nothing but the static binary) ------------------------
FROM scratch

COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/dockdoe /dockdoe
# Pre-create the data dir so the volume mountpoint exists even on scratch.
COPY --from=builder /out/data /data

# Bind on all interfaces inside the container; DB lives in the /data volume.
ENV DOCKDOE_BIND=0.0.0.0:8080 \
    DOCKDOE_DB_PATH=/data/dockdoe.sqlite

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/dockdoe"]
