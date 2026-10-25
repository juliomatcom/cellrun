# syntax=docker/dockerfile:1
# Orchestrator image: static-ish Rust binary on distroless (glibc, no shell).
#
#   docker build -t hostprobe/orchestrator:latest apps/orchestrator

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin orchestrator \
 && cp target/release/orchestrator /orchestrator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /orchestrator /usr/local/bin/orchestrator
COPY examples /examples
ENTRYPOINT ["/usr/local/bin/orchestrator"]
CMD ["--help"]
