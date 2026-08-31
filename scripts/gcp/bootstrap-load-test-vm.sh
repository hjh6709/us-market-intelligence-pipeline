#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl docker.io docker-compose-v2 git openjdk-21-jre-headless
systemctl enable --now docker
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
touch /var/lib/pipeline-load-test-bootstrap-complete
