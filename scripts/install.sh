#!/bin/bash
# Syncropel CX Shell Installer
#
# This script installs the Syncropel CX Shell by leveraging the Nix package manager.
# It ensures all dependencies are handled correctly, providing a fully reproducible
# installation.

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

    # 2. Ensure the user's Nix configuration supports flakes (a common prerequisite)
    info "Checking for Nix Flakes support..."
    if ! nix flake --version &> /dev/null; then
         warn "Your Nix installation may not have flakes enabled."
         warn "The installer will attempt to proceed, but if it fails, please enable flakes by adding:"
         warn "'experimental-features = nix-command flakes' to your nix.conf file."
         echo
    fi

    # 3. Install the cx-shell from the main GitHub repository using `nix profile`
    info "Installing Syncropel CX Shell from github:syncropel/cx-shell..."
    # This command installs the `default` package from our flake into the user's
    # active profile (usually ~/.nix-profile), making `cx` available everywhere.
    # It fetches the latest commit from the `main` branch.
    nix profile install github:syncropel/cx-shell

    echo
    success "Syncropel CX Shell installed successfully!"
    info "To get started, please open a NEW terminal session."
    info "You can then set up your workspace by running:"
    info "  cx init"
}

# Run the main function with all arguments passed to the script
main "$@"