#!/bin/bash
# Syncropel CX Shell Installer v2.0
#
# This script uses the modern `nix profile` command suite and includes
# a robust verification loop that queries the Nix profile directly,
# ensuring reliability across different environments.

set -e

# --- Helper Functions for Colored Output ---
info() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}
success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
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

    # 2. Find and source the Nix profile script for the current session.
    #    This makes the installer's own environment robust.
    if [ -e "$HOME/.nix-profile/etc/profile.d/nix.sh" ]; then
        info "Sourcing Nix environment from single-user profile (~/.nix-profile)..."
        . "$HOME/.nix-profile/etc/profile.d/nix.sh"
    elif [ -e "/etc/profile.d/nix.sh" ]; then
        info "Sourcing Nix environment from multi-user profile (/etc/profile.d)..."
        . "/etc/profile.d/nix.sh"
    else
        error "Could not find the Nix profile script in standard locations."
        error "Your Nix installation might be non-standard. Please ensure your shell is correctly configured for Nix."
        exit 1
    fi

    # 3. Install the cx-shell from the GitHub repository
    local package_identifier="github:syncropel/cx-shell"
    info "Installing Syncropel CX Shell from ${package_identifier}..."
    info "This may take a few minutes as Nix builds the environment..."
    nix profile add "${package_identifier}" --verbose

    # 4. Robust Verification Step using `nix profile list`
    info "Verifying installation..."

    max_wait_seconds=15
    elapsed=0
    # Loop until `nix profile list` shows our package identifier
    while ! nix profile list | grep -q "${package_identifier}"; do
        if [ "$elapsed" -ge "$max_wait_seconds" ]; then
            error "Installation failed! Package was not found in 'nix profile list' after ${max_wait_seconds} seconds."
            error "Please review the output from the 'nix profile add' command for errors."
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
    done
    echo # Newline after the dots

    success "Verified that package '${package_identifier}' is in the Nix profile."
    echo
    success "Syncropel CX Shell installed successfully!"
    info "To get started, please open a NEW terminal session, then run:"
    info "  cx init"
}

# Run the main function
main "$@"