#!/bin/bash
# Syncropel CX Shell Installer v1.3
#
# This script is designed to be maximally robust, creating its own
# environment to ensure a successful installation even if the user's
# shell profile is not yet configured for Nix.

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

    # --- START OF DEFINITIVE FIX ---
    # 2. Find the user's Nix profile and source it for the current script session.
    # This makes the script self-contained and not reliant on the user's current PATH.
    NIX_PROFILE_DIR=""
    # The `nix` executable is usually in a path like /nix/var/nix/profiles/default/bin/nix
    # We want to find the user-specific profile, typically ~/.nix-profile
    if [ -f "$HOME/.nix-profile/etc/profile.d/nix.sh" ]; then
        info "Sourcing Nix environment from ~/.nix-profile..."
        . "$HOME/.nix-profile/etc/profile.d/nix.sh"
        NIX_PROFILE_DIR="$HOME/.nix-profile"
    else
        error "Could not find the Nix profile script. Your Nix installation might be non-standard."
        exit 1
    fi
    # --- END OF DEFINITIVE FIX ---

    # 3. Install the cx-shell from the GitHub repository
    info "Installing Syncropel CX Shell from github:syncropel/cx-shell..."
    info "This may take a few minutes the first time as Nix builds the environment..."
    nix profile add github:syncropel/cx-shell --verbose

    # 4. Verification Step using the discovered profile path
    info "Verifying installation..."
    CX_PATH="$NIX_PROFILE_DIR/bin/cx"

    max_wait_seconds=10
    elapsed=0
    while [ ! -L "$CX_PATH" ]; do
        if [ "$elapsed" -ge "$max_wait_seconds" ]; then
            error "Installation failed! The 'cx' command was not found in your Nix profile bin directory after ${max_wait_seconds} seconds."
            error "Please check the output above for errors from the 'nix profile add' command."
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
    done
    echo

    success "Verified that '$CX_PATH' symlink exists."
    echo
    success "Syncropel CX Shell installed successfully!"
    info "To get started, please open a NEW terminal session, then run:"
    info "  cx init"
}

# Run the main function
main "$@"