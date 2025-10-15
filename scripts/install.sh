#!/bin/bash
# Syncropel CX Shell Installer v1.2
#
# This script installs the Syncropel CX Shell by leveraging the Nix package manager.
# It uses modern, non-deprecated commands and includes a robust, patient
# verification step to handle installation latency.

set -e

# --- Helper Functions for Colored Output ---
info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}
success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}
warn() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}
error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1" >&2
}

# --- Main Installation Logic ---
main() {
    info "Welcome to the Syncropel CX Shell Installer!"
    info "This script will install 'cx' into your user profile using Nix."
    echo

    # 1. Check if Nix is installed
    if ! command -v nix &> /dev/null; then
        error "Nix is not installed on your system. Please install it to continue."
        info "You can install Nix by visiting https://nixos.org/download.html"
        info "After installing Nix, please re-run this script."
        exit 1
    fi
    success "Nix installation found."

    # 2. Install the cx-shell from the GitHub repository
    info "Installing Syncropel CX Shell from github:syncropel/cx-shell..."
    info "This may take a few minutes the first time as Nix builds the environment..."
    # The `nix profile add` command is blocking and will wait for completion.
    nix profile add github:syncropel/cx-shell --verbose

    # 3. Robust Verification Step
    info "Verifying installation..."
    CX_PATH="$HOME/.nix-profile/bin/cx"

    # --- THE FIX: Patiently wait for the symlink to appear ---
    # We will check for up to 10 seconds for the symlink to be created by the
    # background Nix daemon, making the script resilient to system load.
    max_wait_seconds=10
    elapsed=0
    while [ ! -L "$CX_PATH" ]; do
        if [ "$elapsed" -ge "$max_wait_seconds" ]; then
            error "Installation failed! The 'cx' command was not found in ~/.nix-profile/bin after ${max_wait_seconds} seconds."
            error "Please check the output above for errors from the 'nix profile add' command."
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
    done
    echo # Newline after the dots

    success "Verified that '$CX_PATH' symlink exists."

    echo
    success "Syncropel CX Shell installed successfully!"
    info "To get started, please open a NEW terminal session, then run:"
    info "  cx init"
}

# Run the main function
main "$@"