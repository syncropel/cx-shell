#!/bin/bash
# Syncropel CX Shell Installer v1.4
#
# This script is designed to be maximally robust, supporting both single-user
# and multi-user Nix installations by searching for the profile script in
# standard locations.

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

    # --- START OF DEFINITIVE FIX for MULTI-USER NIX ---
    # 2. Find and source the Nix profile script for the current session.
    #    This makes the installer self-contained and robust.
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
    # --- END OF DEFINITIVE FIX ---

    # 3. Install the cx-shell from the GitHub repository
    info "Installing Syncropel CX Shell from github:syncropel/cx-shell..."
    info "This may take a few minutes the first time as Nix builds the environment..."
    nix profile add github:syncropel/cx-shell --verbose

    # 4. Robust Verification Step
    info "Verifying installation..."
    # Even in multi-user mode, `nix profile` commands create/update the symlink at `~/.nix-profile`
    CX_PATH="$HOME/.nix-profile/bin/cx"

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