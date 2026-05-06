#!/bin/bash
# Build Spark+PySpark Docker image with Python and profiling tools.
# Expected environment variables:
#   IMAGE_NAME - target image name (e.g. pyspark:3.5.0-py311-arm)
#   BASE_IMAGE - base image to build from (e.g. spark:3.5.0)
#   PYTHON_VERSION - Python version to install (e.g. 3.11)
#   WORKER_COUNT - number of worker containers (for metadata only)

set -e

IMAGE_NAME="${IMAGE_NAME:-spark:3.5.0-py311}"
BASE_IMAGE="${BASE_IMAGE:-spark:3.5.0}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
WORKER_COUNT="${WORKER_COUNT:-2}"

echo "[build-spark-image] Building $IMAGE_NAME from $BASE_IMAGE"
echo "  Python version: $PYTHON_VERSION"
echo "  Worker count: $WORKER_COUNT"

# Create Dockerfile
cat > /tmp/Dockerfile.spark << 'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG PYTHON_VERSION=3.11

# Install system dependencies, Python, and profiling tools
RUN apt-get update && apt-get install -y \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-dev \
    python${PYTHON_VERSION}-distutils \
    python${PYTHON_VERSION}-venv \
    build-essential \
    curl \
    wget \
    git \
    linux-tools-generic \
    binutils \
    strace \
    gdb \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Symlink python3 to the specific version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1

# Verify Python and profiling tools
RUN python3 --version && perf --version && objdump --version

WORKDIR /opt/spark
ENTRYPOINT ["/opt/spark/bin/spark-class"]
EOF

# Build the image
docker build \
  --build-arg BASE_IMAGE="${BASE_IMAGE}" \
  --build-arg PYTHON_VERSION="${PYTHON_VERSION}" \
  -t "${IMAGE_NAME}" \
  -f /tmp/Dockerfile.spark \
  /tmp

echo "[build-spark-image] ✓ Image built: $IMAGE_NAME"
rm -f /tmp/Dockerfile.spark
