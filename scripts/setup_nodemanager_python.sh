#!/usr/bin/env bash
# =============================================================================
# Installs python3 inside the NodeManager container.
#
# WHY THIS IS NEEDED
#   Hadoop Streaming runs mapper.py / reducer.py as subprocesses on the
#   NodeManager, so a Python interpreter must exist inside THAT container.
#   The bde2020 images ship without one.
#
# WHY IT IS A SCRIPT AND NOT A DOCKERFILE
#   A Dockerfile would be the cleaner answer, but `docker build` runs with an
#   isolated network on this setup and cannot resolve DNS, so apt fails at
#   build time. At RUNTIME (docker compose exec) DNS works fine, which is why
#   this approach succeeds where the build did not.
#
# WHY THE MIRROR IS REWRITTEN
#   These images are Debian 9 (stretch), which is end-of-life. Its packages
#   moved to archive.debian.org and the original mirrors now return 404.
#
# IMPORTANT
#   The install lives only in the running container. Re-run this script after
#   any `docker compose up -d` that RECREATES the nodemanager (a plain restart
#   is fine). It is idempotent -- safe to run any number of times.
#
# Usage:  bash scripts/setup_nodemanager_python.sh
# =============================================================================
set -uo pipefail

NM="${NM_SERVICE:-nodemanager}"

echo "=============================================================="
echo " Installing python3 into the '$NM' container"
echo "=============================================================="

if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$NM"; then
  echo "ERROR: the '$NM' service is not running." >&2
  echo "Start it first:  docker compose up -d" >&2
  exit 1
fi

if docker compose exec -T "$NM" bash -c "command -v python3 >/dev/null 2>&1"; then
  VER=$(docker compose exec -T "$NM" python3 --version 2>&1 | tr -d '\r')
  echo ">>> Already installed: $VER"
  echo ">>> Verifying the csv module (needed to parse quoted amounts)"
  docker compose exec -T "$NM" python3 -c "import csv; print('    csv module OK')"
  exit 0
fi

echo ">>> Not present -- installing from archive.debian.org"
docker compose exec -T -u root "$NM" bash -c '
  set -e
  printf "%s\n" \
    "deb http://archive.debian.org/debian stretch main" \
    "deb http://archive.debian.org/debian-security stretch/updates main" \
    > /etc/apt/sources.list
  apt-get -o Acquire::Check-Valid-Until=false update -qq
  apt-get install -y --no-install-recommends python3
' || {
  echo >&2
  echo "ERROR: install failed." >&2
  echo "Check the container has network access:" >&2
  echo "  docker compose exec $NM ping -c1 archive.debian.org" >&2
  exit 1
}

echo
echo ">>> Verifying"
docker compose exec -T "$NM" python3 --version
docker compose exec -T "$NM" python3 -c "import csv; print('csv module OK')"

echo
echo "Done. Note: python3 3.5.x has no f-strings, which is why mapper.py and"
echo "reducer.py use .format() instead."
echo
echo "Next: bash mapreduce/run_mr.sh"
