#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates docker.io docker-compose-v2 git openjdk-21-jre-headless python3-pip
systemctl enable --now docker
python3 -m pip install --break-system-packages uv==0.10.12
touch /var/lib/pipeline-load-test-bootstrap-complete
