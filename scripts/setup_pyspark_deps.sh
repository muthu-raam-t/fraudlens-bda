#!/usr/bin/env bash
# =============================================================================
# Installs the Python packages PySpark MLlib needs INSIDE the cluster
# containers.
#
# WHY
#   pyspark.ml imports numpy at module load time. The bde2020 images ship a
#   bare Python with no scientific stack, so `from pyspark.ml import Pipeline`
#   dies with ModuleNotFoundError before any job starts.
#
# WHICH CONTAINERS
#   spark-master : runs the DRIVER (deploy-mode client)
#   nodemanager  : runs the EXECUTOR containers YARN launches
#   spark-worker : standalone worker, kept consistent so ad-hoc jobs work
#
# WHY NOT pip
#   These images are old (Python 3.5 / musl or Debian 9). There is no matching
#   numpy wheel, so pip would try to COMPILE numpy from source without a
#   toolchain. The distro package is prebuilt and installs in seconds.
#
# NOTE
#   Installs live only in the running container. Re-run after any
#   `docker compose up -d` that RECREATES these services. Idempotent.
#
# Usage: bash scripts/setup_pyspark_deps.sh
# =============================================================================
set -uo pipefail

SERVICES="${PYSPARK_DEP_SERVICES:-spark-master spark-worker nodemanager}"

echo "=============================================================="
echo " Installing PySpark dependencies (numpy) in cluster containers"
echo "=============================================================="

install_into() {
  local svc="$1"

  if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$svc"; then
    echo ">>> $svc: NOT RUNNING -- skipping"
    return 0
  fi

  echo
  echo ">>> $svc"

  # Already there?
  if docker compose exec -T "$svc" python3 -c "import numpy" >/dev/null 2>&1; then
    local v
    v=$(docker compose exec -T "$svc" python3 -c \
        "import numpy; print(numpy.__version__)" 2>/dev/null | tr -d '\r')
    echo "    numpy already installed ($v)"
    return 0
  fi

  # Which package manager does this image use?
  if docker compose exec -T "$svc" sh -c "command -v apk" >/dev/null 2>&1; then
    echo "    Alpine detected -- apk add py3-numpy"
    docker compose exec -T -u root "$svc" sh -c \
      "apk add --no-cache py3-numpy" >/dev/null 2>&1
  elif docker compose exec -T "$svc" sh -c "command -v apt-get" >/dev/null 2>&1; then
    echo "    Debian detected -- apt-get install python3-numpy"
    docker compose exec -T -u root "$svc" bash -c '
      set -e
      if grep -q "stretch" /etc/apt/sources.list 2>/dev/null; then
        printf "%s\n" \
          "deb http://archive.debian.org/debian stretch main" \
          "deb http://archive.debian.org/debian-security stretch/updates main" \
          > /etc/apt/sources.list
      fi
      apt-get -o Acquire::Check-Valid-Until=false update -qq
      apt-get install -y --no-install-recommends python3-numpy
    ' >/dev/null 2>&1
  else
    echo "    ERROR: neither apk nor apt-get found in $svc" >&2
    return 1
  fi

  if docker compose exec -T "$svc" python3 -c "import numpy" >/dev/null 2>&1; then
    local v
    v=$(docker compose exec -T "$svc" python3 -c \
        "import numpy; print(numpy.__version__)" 2>/dev/null | tr -d '\r')
    echo "    installed numpy $v"
  else
    echo "    FAILED to install numpy in $svc" >&2
    return 1
  fi
}

FAILED=0
for svc in $SERVICES; do
  install_into "$svc" || FAILED=1
done

echo
echo "=============================================================="
echo " Verification"
echo "=============================================================="
for svc in $SERVICES; do
  if docker compose ps --status running --services 2>/dev/null | grep -qx "$svc"; then
    printf '%-16s ' "$svc"
    docker compose exec -T "$svc" python3 -c \
      "import numpy, sys; print('python %s, numpy %s' % (sys.version.split()[0], numpy.__version__))" \
      2>/dev/null | tr -d '\r' || echo "numpy MISSING"
  fi
done

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All good. Next: bash scripts/run_train.sh"
else
  echo "One or more installs failed -- see the messages above." >&2
  exit 1
fi
