# /flake.nix
{
  description = "The Syncropel Context Executor (cx) Shell, built with a Hybrid Nix+UV model";

  inputs = {
    # Pinning nixpkgs ensures that every developer and every CI run
    # uses the exact same set of system packages for perfect reproducibility.
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      # Define the systems we support for building and developing.
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      # A helper function to generate attribute sets for each supported system.
      forEachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      # ========================================================================
      #   1. PACKAGES: For building distributable artifacts (nix build)
      # ========================================================================
      packages = forEachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.stdenv.mkDerivation rec {
            pname = "cx-shell";
            version = "0.1.1";

            src = ./.;

            buildInputs = with pkgs; [ stdenv.cc.cc.lib zlib xz unixODBC ];

            nativeBuildInputs = with pkgs; [
              python312 uv git cacert autoPatchelfHook
              stdenv.cc.cc.lib zlib.dev xz.dev unixODBC
            ];

            buildPhase = ''
              runHook preBuild
              export HOME=$TMPDIR
              export UV_CACHE_DIR=$TMPDIR/.uv-cache
              export SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
              
              ${pkgs.uv}/bin/uv venv $TMPDIR/venv --python ${pkgs.python312}/bin/python
              source $TMPDIR/venv/bin/activate
              ${pkgs.uv}/bin/uv pip install ".[all]"
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out/libexec
              cp -r $TMPDIR/venv $out/libexec/cx-shell-env
              
              find $out/libexec/cx-shell-env -type f -name "*.pyc" -delete
              find $out/libexec/cx-shell-env -type d -name "__pycache__" -exec rm -rf {} +

              mkdir -p $out/bin
              cat > $out/bin/cx <<EOF
              #!${pkgs.bash}/bin/bash
              exec $out/libexec/cx-shell-env/bin/python -m cx_shell.main "\$@"
              EOF
              chmod +x $out/bin/cx
              runHook postInstall
            '';

            __noChroot = true;
          };

          container = pkgs.dockerTools.buildLayeredImage {
            name = "cx-shell";
            tag = "latest";
            contents = with pkgs; [ self.packages.${system}.default stdenv.cc.cc.lib zlib xz unixODBC git ];
            config = {
              Entrypoint = [ "${self.packages.${system}.default}/bin/cx" ];
              Cmd = [ "--help" ];
              WorkingDir = "/app";
              Env = [
                "PATH=${pkgs.lib.makeBinPath [ pkgs.git self.packages.${system}.default ]}"
                "PYTHONUNBUFFERED=1"
              ];
            };
          };

          docker-load = pkgs.writeShellScriptBin "docker-load" ''
            set -e; nix build .#container; docker load < result; echo "✅ Loaded image cx-shell:latest"
          '';
          docker-push = pkgs.writeShellScriptBin "docker-push" ''
            set -e
            if [ -z "$1" ]; then echo "Usage: nix run .#docker-push -- <registry/image>"; exit 1; fi
            REGISTRY_IMAGE="$1"; VERSION="${self.packages.${system}.default.version}"
            nix build .#container; docker load < result
            docker tag cx-shell:latest "$REGISTRY_IMAGE:$VERSION"; docker tag cx-shell:latest "$REGISTRY_IMAGE:latest"
            docker push "$REGISTRY_IMAGE:$VERSION"; docker push "$REGISTRY_IMAGE:latest"
            echo "✅ Pushed to $REGISTRY_IMAGE"
          '';
        }
      );

      # ========================================================================
      #   2. APPS: For running packages directly (nix run)
      # ========================================================================
      apps = forEachSystem (system: {
        default = { type = "app"; program = "${self.packages.${system}.default}/bin/cx"; };
        docker-load = { type = "app"; program = "${self.packages.${system}.docker-load}/bin/docker-load"; };
        docker-push = { type = "app"; program = "${self.packages.${system}.docker-push}/bin/docker-push"; };
      });

      # ========================================================================
      #   3. DEVSHELLS: For the development environment (nix develop)
      # ========================================================================
      devShells = forEachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };

          runtimeLibs = with pkgs; [ stdenv.cc.cc.lib zlib xz unixODBC ];
          playwrightDeps = with pkgs; [
            glib nspr nss dbus atk at-spi2-atk cups expat libxcb libxkbcommon
            xorg.libX11 xorg.libXcomposite xorg.libXdamage xorg.libXext xorg.libXfixes xorg.libXrandr
            mesa cairo pango systemd alsa-lib
          ];
          coreTools = with pkgs; [ uv python312 git docker kubectl ];

        in
        {
          default = pkgs.mkShell {
            buildInputs = coreTools ++ runtimeLibs ++ playwrightDeps;

            shellHook = ''
              export PS1="\n\[\033[1;32m\][nix-cx-shell]\[\033[0m\] \[\033[1;34m\]\w\[030m\]\n\$ "
              
              export NIX_LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath (runtimeLibs ++ playwrightDeps)}''${NIX_LD_LIBRARY_PATH:+:$NIX_LD_LIBRARY_PATH}"
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath (runtimeLibs ++ playwrightDeps)}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              
              echo "--- CX Shell Development Environment ---"
              echo "Using Python interpreter from Nix: ${pkgs.python312}/bin/python3"
              
              if [ ! -d ".venv" ]; then
                echo "Creating Python virtual environment at: .venv"
                uv venv
              fi
              
              source .venv/bin/activate
              
              if [ ! -f ".venv/.installed" ]; then 
                echo "Installing Python dependencies for the first time..."
                uv pip install -e '.[all]'
                echo "Downloading and installing Playwright browser binaries..."
                playwright install chromium
                touch .venv/.installed
              fi
              
              echo "✅ Environment is ready to develop!"
            '';
          };
        }
      );
    };
}