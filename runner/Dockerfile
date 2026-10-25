# Runner image: the environment each ephemeral task container starts from.
# Debian + common dev tooling + opencode (https://opencode.ai).
#
#   docker build -t hostprobe/opencode-runner:latest apps/orchestrator/runner
#
# Provider credentials (e.g. ANTHROPIC_API_KEY) are NOT baked in; the
# orchestrator injects them per task via `env_passthrough`.
#
# Runs as root on purpose: the workload (opencode) has full access *inside*
# its container. Isolation from the host and other tasks comes from the
# orchestrator (own network per task, no mounts, sandbox-guard firewall,
# optionally gVisor), not from restricting the user.
FROM debian:bookworm-slim

ARG OPENCODE_VERSION=1.19.4

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      bash ca-certificates curl git jq less openssh-client procps ripgrep tar unzip \
 && rm -rf /var/lib/apt/lists/*

# Install a pinned opencode build system-wide, then drop the installer leftovers.
RUN curl -fsSL https://opencode.ai/install \
      | bash -s -- --version "${OPENCODE_VERSION}" --no-modify-path \
 && install -m 0755 /root/.opencode/bin/opencode /usr/local/bin/opencode \
 && rm -rf /root/.opencode \
 && opencode --version

WORKDIR /workspace
