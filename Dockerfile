# syntax=docker/dockerfile:1
#
# kibbr as one static binary on distroless: no shell, no libc, no package manager, and
# not root.
#
#   docker build -t kibbr .
#   docker run -d --name kibbr --stop-timeout 30 -p 7700:7700 -v kibbr-data:/var/lib/kibbr kibbr serve
#
# **`--stop-timeout 30`, and not Docker's ten seconds.** A stop gives the requests in
# flight 25 seconds (`[api] shutdown_deadline`) and then runs the final flush, the
# checkpoint and the trie snapshot. Killed at ten, nothing acknowledged is lost - it is
# in the write-ahead log - but the next start reads the log back. Kubernetes' default
# grace is the thirty this is built for.
#
# The build is for the machine it runs on: an arm64 image on an arm64 host, amd64 on
# amd64. Nothing is cross-compiled.

# ── build ──────────────────────────────────────────────────────────────────────────
# Alpine, because its Rust targets musl and links it statically: the binary that comes
# out needs nothing of the image it was built in. musl-dev is the C library's headers
# and startup files for the two C dependencies, libzstd and mimalloc; gcc is in the base.
FROM rust:1.96-alpine AS build
RUN apk add --no-cache musl-dev file
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# The two library crates, and of crates/kibbr-devtools its manifest alone: .dockerignore
# lets nothing else of the bench into the context, and Cargo asks for nothing else of it.
COPY crates crates
COPY src src
# The registry and the target directory are cache mounts: they outlive the layer, so a
# change to src/ compiles what changed and not every dependency again. The binary is
# copied out of the mount before the layer ends, or the next stage would not find it.
#
# `--locked`: the versions in Cargo.lock, or no image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin kibbr \
 && mkdir -p /out/var/lib/kibbr \
 && cp target/release/kibbr /out/kibbr \
 # A build that came out dynamic would start on Alpine and nowhere else, and say so
 # only when the image is run. `file` says which it is; this stops the build there.
 && file /out/kibbr | tee /dev/stderr | grep -Eq 'statically linked|static-pie linked' \
 && /out/kibbr --version

# ── run ────────────────────────────────────────────────────────────────────────────
# The last stage, so `docker build .` with no --target is this one.
# distroless/static: CA certificates, tzdata, /etc/passwd with `nonroot` (65532), /tmp,
# and nothing that runs. `:nonroot` makes that user the default; it is named below as
# numbers too, which is what a Kubernetes `runAsNonRoot` check can verify.
FROM gcr.io/distroless/static-debian12:nonroot

ARG KIBBR_COMMIT=unknown
LABEL org.opencontainers.image.title="kibbr" \
      org.opencontainers.image.description="kibbr - log store" \
      org.opencontainers.image.source="https://github.com/pratyaydeep/kibbr" \
      org.opencontainers.image.revision="${KIBBR_COMMIT}"

COPY --from=build /out/kibbr /usr/local/bin/kibbr
# The volume's directory, made in the image and owned by the user the server runs as: a
# new named volume takes its owner from here. A bind mount or a Kubernetes volume does
# not - there the owner is the host directory's, or `fsGroup: 65532`.
COPY --from=build --chown=65532:65532 /out/var/lib/kibbr /var/lib/kibbr

# The settings a container needs to differ in, each one a variable a flag still wins
# over: the data under the volume; every interface, because loopback
# inside a container is reachable by nothing outside it; and the log as JSON, one
# object a line, for whatever collects the container's stderr.
ENV KIBBR__STORAGE__BASE_PATH=/var/lib/kibbr/data \
    KIBBR__STORAGE__TRIE_FILE=/var/lib/kibbr/trie.json \
    KIBBR__API__NATIVE_ADDR=0.0.0.0:7700 \
    KIBBR__LOG__FORMAT=json

USER 65532:65532
WORKDIR /var/lib/kibbr
VOLUME /var/lib/kibbr
EXPOSE 7700
STOPSIGNAL SIGTERM

# `kibbr health` is GET /health asked by the binary itself: there is no curl here to ask
# it. Exit 0 while the server answers `"ok":true`, which it does when degraded too - a
# server that cannot write still answers queries, and restarting it does not empty a
# disk. The start period is long because a start after a kill reads the log back before
# it listens.
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=3 \
    CMD ["/usr/local/bin/kibbr", "health"]

ENTRYPOINT ["/usr/local/bin/kibbr"]
CMD ["serve"]
