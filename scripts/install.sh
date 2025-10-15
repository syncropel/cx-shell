#!/bin/bash
# Syncropel CX Shell Installer v1.1
#
# This script installs the Syncropel CX Shell by leveraging the Nix package manager.
# It uses modern, non-deprecated commands and includes verification steps.

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

    # 2. Add Nix profile to the shell if it's missing (for the CURRENT session)
    # This makes the script more robust if the user's .bashrc isn't set up yet.
    if [[ ":$PATH:" != *":$HOME/.nix-profile/bin:"* ]]; then
        info "Temporarily adding Nix profile to PATH for this session."
        export PATH="$HOME/.nix-profile/bin:$PATH"
    fi

    # 3. Install the cx-shell from the GitHub repository using the modern `add` command
    info "Installing Syncropel CX Shell from github:syncropel/cx-shell..."
    # The modern, correct command is `nix profile add`. We also add `--verbose` for better debugging.
    nix profile add github:syncropel/cx-shell --verbose

    # 4. Verification Step
    info "Verifying installation..."
    CX_PATH="$HOME/.nix-profile/bin/cx"

    if [ -L "$CX_PATH" ]; then
        success "Verified that '$CX_PATH' symlink exists."
    else
        error "Installation failed! The 'cx' command was not found in ~/.nix-profile/bin."
        error "Please check the output above for errors from the 'nix profile add' command."
        exit 1
    fi

    echo
    success "Syncropel CX Shell installed successfully!"
    info "To get started, please open a NEW terminal session."
    info "You can then set up your workspace by running:"
    info "  cx init"
}

# Run the main function
main "$@"