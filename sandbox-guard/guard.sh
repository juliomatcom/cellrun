#!/bin/sh
# Host firewall for task containers. Idempotent; safe to run on every start.
#
# The orchestrator names every task bridge `<prefix><hex>` (prefix `sw` by
# default, `--bridge-prefix` there), so one interface wildcard (`<prefix>+`)
# covers all of them. Set BRIDGE_PREFIX to match the orchestrator:
#   INPUT        -i sw+ -> DROP   task containers cannot reach ANY host address
#                                 (bridge gateway, docker0, public/LAN IPs).
#   DOCKER-USER  -i sw+ -> DROP   for private/link-local/CGNAT destinations:
#                                 no LAN, no cloud metadata, no other containers.
# Public internet (via Docker's NAT) stays open so agents like Goose work.
#
# Usage: guard.sh [apply|remove]   (needs CAP_NET_ADMIN in the host netns)
set -eu

ACTION=${1:-apply}
IFACE="${BRIDGE_PREFIX:-sw}+"
PRIVATE='10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,100.64.0.0/10,0.0.0.0/8,224.0.0.0/4,240.0.0.0/4'

# Use whichever backend Docker programs (it owns the DOCKER-USER chain).
pick() {
    for bin in "$@"; do
        if command -v "$bin" >/dev/null 2>&1 && "$bin" -S DOCKER-USER >/dev/null 2>&1; then
            echo "$bin"
            return 0
        fi
    done
    return 1
}
IPT=$(pick iptables-nft iptables-legacy iptables) || {
    echo "sandbox-guard: no DOCKER-USER chain found; is Docker running with its iptables firewall?" >&2
    exit 1
}
IP6T=$(pick "${IPT%%tables*}6tables${IPT#*iptables}" ip6tables || true)

rule() { # rule <bin> <chain> <args...>: insert once (apply) or delete (remove)
    bin=$1 chain=$2
    shift 2
    if "$bin" -C "$chain" "$@" 2>/dev/null; then
        [ "$ACTION" = remove ] && "$bin" -D "$chain" "$@"
    else
        [ "$ACTION" = apply ] && "$bin" -I "$chain" 1 "$@"
    fi
    return 0
}

rule "$IPT" INPUT -i "$IFACE" -j DROP
rule "$IPT" DOCKER-USER -i "$IFACE" -d "$PRIVATE" -j DROP
if [ -n "$IP6T" ]; then
    # Task networks have IPv6 disabled; this is defense in depth.
    rule "$IP6T" INPUT -i "$IFACE" -j DROP
    rule "$IP6T" DOCKER-USER -i "$IFACE" -j DROP
fi

echo "sandbox-guard: $ACTION ok ($IPT${IP6T:+, $IP6T})"
