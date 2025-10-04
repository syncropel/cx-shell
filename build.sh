#!/bin/bash
# This script builds the `cx` executable for the current platform (Linux/macOS).
# It uses the `cx.spec` file as the single source of truth for the build.
set -e

echo "--- Cleaning up old build artifacts ---"
rm -rf dist/
rm -rf build/

echo
echo "--- Building the single-file executable using cx.spec ---"
pyinstaller cx.spec

echo
echo "--- Build complete! ---"
echo "The self-contained executable is located at: dist/cx"
echo "You can test it by running: ./dist/cx init"