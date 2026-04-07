#!/bin/bash
# -----------------------------------------------------------------------------
# Custom Dependency Installation Script
# -----------------------------------------------------------------------------
# This script is executed during the final stage of the Docker build process.
# Use this file to install any additional system packages required by your
# application, avoiding the need to modify the Dockerfile directly.
#
# IMPORTANT: Since this runs in a Debian-based image, use apt-get.
# -----------------------------------------------------------------------------

set -e

echo "Starting custom dependency installation..."

# Uncomment and modify the lines below to install your required packages:

# apt-get update
# apt-get install -y --no-install-recommends \
#     ffmpeg \
#     nodejs \
#     npm \
#     playwright \
#     pkg-config

echo "Custom dependency installation completed."
