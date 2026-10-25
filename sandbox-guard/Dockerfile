# One-shot container that installs the host firewall rules isolating task
# containers (see guard.sh). Runs with network_mode: host + NET_ADMIN.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends iptables \
 && rm -rf /var/lib/apt/lists/*
COPY guard.sh /usr/local/bin/guard.sh
ENTRYPOINT ["/usr/local/bin/guard.sh"]
CMD ["apply"]
