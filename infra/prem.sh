#!/usr/bin/env bash
# Provision nginx + Let's Encrypt for an Ochi server on Fedora.
#
# Usage (run as root after DNS has propagated):
#   DOMAIN=demo.ochi.dev EMAIL=you@example.com ./prem.sh
#
# Optional: BACKEND=127.0.0.1:9014 ./prem.sh

set -euo pipefail

: "${DOMAIN:?Set DOMAIN, e.g. DOMAIN=demo.ochi.dev}"
: "${EMAIL:?Set EMAIL for certificate expiry notices}"
BACKEND="${BACKEND:-127.0.0.1:9014}"

if [[ ${EUID} -ne 0 ]]; then
    echo "Run this script as root." >&2
    exit 1
fi

if ! command -v dnf >/dev/null; then
    echo "This provisioning script currently targets Fedora (dnf)." >&2
    exit 1
fi

# TODO: Automate the Cloudflare DNS step before Certbot runs. Create an A record
# for $DOMAIN pointing to this machine's public IPv4 address. Do not create a
# conflicting AAAA or CNAME record. Cloudflare's API token should be supplied via
# a secret, never committed to this repository.
#
# Ochi and Vector are installed separately. Keep them under systemd so they
# start at boot and are restarted if either process exits unexpectedly.
OCHI_BIN=/root/ochi
VECTOR_CONFIG=/root/vector-loki-fmt-json.yaml

if [[ ! -x $OCHI_BIN ]]; then
    echo "Ochi binary is missing or not executable: $OCHI_BIN" >&2
    exit 1
fi

if [[ ! -f $VECTOR_CONFIG ]]; then
    echo "Vector configuration is missing: $VECTOR_CONFIG" >&2
    exit 1
fi

VECTOR_BIN="$(command -v vector || true)"
if [[ -z $VECTOR_BIN ]]; then
    echo "Vector is not installed or is not on PATH." >&2
    exit 1
fi

cat > /etc/systemd/system/ochi.service <<EOF
[Unit]
Description=Ochi log query server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${OCHI_BIN}
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/ochi-vector.service <<EOF
[Unit]
Description=Vector collector for Ochi
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${VECTOR_BIN} --config ${VECTOR_CONFIG}
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now ochi.service ochi-vector.service

dnf install -y nginx certbot python3-certbot-nginx nftables

# nginx on Fedora is configured through /etc/nginx/conf.d rather than Debian's
# sites-available/sites-enabled layout. Remove the packaged default HTTP server.
sed -i '/^    server {$/,/^    }$/d' /etc/nginx/nginx.conf

cat > /etc/nginx/conf.d/ochi.conf <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN};

    location / {
        # Allow the separately hosted production web UI to POST /query.
        add_header Access-Control-Allow-Origin "*" always;
        add_header Access-Control-Allow-Methods "POST, OPTIONS" always;
        add_header Access-Control-Allow-Headers "Content-Type" always;
        if (\$request_method = OPTIONS) { return 204; }

        proxy_pass http://${BACKEND};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 300s;
    }
}
EOF

# SELinux otherwise prevents nginx from connecting to the localhost upstream.
setsebool -P httpd_can_network_connect on

# Permit localhost to reach Ochi, but drop all network traffic directed at its
# backend port. The default input policy stays accept, so ports 80/443 remain
# open. Configure any provider firewall (for example, DigitalOcean Cloud
# Firewall) to allow TCP 80 and 443 as well.
cat > /etc/sysconfig/nftables.conf <<EOF
table inet ochi_proxy {
    chain input {
        type filter hook input priority filter; policy accept;
        iifname "lo" accept
        tcp dport ${BACKEND##*:} drop
    }
}
EOF
nft -c -f /etc/sysconfig/nftables.conf
systemctl enable --now nftables

nginx -t
systemctl enable --now nginx

# This needs public DNS for $DOMAIN to be live and ports 80/443 reachable.
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m "$EMAIL" --redirect
systemctl enable --now certbot-renew.timer

echo
echo 'Verification:'
curl -sS -I "https://${DOMAIN}/query"
systemctl is-enabled certbot-renew.timer
systemctl is-active certbot-renew.timer
systemctl is-active ochi.service
systemctl is-active ochi-vector.service
ss -ltnp | grep -E ':(80|443|9014)'
nft list table inet ochi_proxy
