# Runner image: the environment each ephemeral task container starts from.
# Debian + common dev tooling + Goose (https://github.com/block/goose), a
# provider-agnostic coding agent (OpenAI, Anthropic, Gemini, OpenRouter, Groq,
# Ollama and any OpenAI-compatible endpoint, so open-weight models too).
#
#   docker build -t swarm/goose-runner:latest runner
#
# Why Goose: same fizzbuzz task, gpt-6-luna, 2 GiB limit — Goose peaked at
# ~144 MiB of process memory vs ~469 MiB for opencode (see README, "Memory").
#
# Provider credentials (e.g. OPENAI_API_KEY) are NOT baked in; the
# orchestrator injects them per task via `env_passthrough`. Provider and
# model are chosen per step: `goose run --provider <p> --model <m> ...`.
#
# Runs as root on purpose: the workload (goose) has full access *inside*
# its container. Isolation from the host and other tasks comes from the
# orchestrator (own network per task, no mounts, sandbox-guard firewall,
# optionally gVisor), not from restricting the user.
FROM debian:bookworm-slim

ARG GOOSE_VERSION=1.56.0

# GOOSE_MODE=auto: run tools without asking (nobody is there to approve).
# GOOSE_DISABLE_KEYRING: there is no OS keychain; keys come from env vars.
ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    GOOSE_MODE=auto \
    GOOSE_DISABLE_KEYRING=1

# bzip2 unpacks the release; libdbus/libxcb are runtime deps of the binary.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      bash bzip2 ca-certificates curl git jq less libdbus-1-3 libxcb1 \
      openssh-client procps ripgrep tar unzip \
 && rm -rf /var/lib/apt/lists/*

# Installer and binary both pinned to the same release.
RUN curl -fsSL "https://github.com/block/goose/releases/download/v${GOOSE_VERSION}/download_cli.sh" \
      | GOOSE_VERSION="${GOOSE_VERSION}" CONFIGURE=false GOOSE_BIN_DIR=/usr/local/bin bash \
 && goose --version

WORKDIR /workspace
