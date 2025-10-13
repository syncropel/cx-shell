# /flake.nix
{
  description = "The Syncropel Context Executor (cx) Shell, built with a Hybrid Nix+UV model";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forEachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.stdenv.mkDerivation rec {
            pname = "cx-shell";
            version = "0.1.1";

            src = ./.;

            buildInputs = with pkgs; [
              stdenv.cc.cc.lib
              zlib
              xz
              unixODBC
            ];

            nativeBuildInputs = with pkgs; [
              python312
              uv
              git # Needed to fetch git dependencies during build
              cacert
              autoPatchelfHook
              stdenv.cc.cc.lib
              zlib.dev
              xz.dev
              unixODBC # unixODBC does not have a separate .dev output
            ];

            buildPhase = ''
              runHook preBuild
              export HOME=$TMPDIR
              export UV_CACHE_DIR=$TMPDIR/.uv-cache
              export SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
              echo "--- Creating virtual environment with uv ---"
              ${pkgs.uv}/bin/uv venv $TMPDIR/venv --python ${pkgs.python312}/bin/python
              source $TMPDIR/venv/bin/activate
              echo "--- Installing Python dependencies with uv ---"
              ${pkgs.uv}/bin/uv pip install ".[all]"
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              echo "--- Copying virtual environment to output ---"
              mkdir -p $out/libexec
              cp -r $TMPDIR/venv $out/libexec/cx-shell-env

              echo "--- Cleaning Python environment ---"
              find $out/libexec/cx-shell-env -type f -name "*.pyc" -delete
              find $out/libexec/cx-shell-env -type d -name "__pycache__" -exec rm -rf {} +

              echo "--- Creating executable wrapper ---"
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
            
            # UPDATED: Add git to the contents of the container
            contents = with pkgs; [
              self.packages.${system}.default
              stdenv.cc.cc.lib
              zlib
              xz
              unixODBC
              git # <--- ADDED
            ];
            
            config = {
              Entrypoint = [ "${self.packages.${system}.default}/bin/cx" ];
              Cmd = [ "--help" ];
              WorkingDir = "/app";
              # UPDATED: Use makeBinPath to correctly build the PATH for the container
              Env = [
                "PATH=${pkgs.lib.makeBinPath [ pkgs.git self.packages.${system}.default ]}" # <--- UPDATED
                "PYTHONUNBUFFERED=1"
              ];
            };
          };

          # Helper scripts
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

      # Apps and DevShells
      apps = forEachSystem (system: {
        default = { type = "app"; program = "${self.packages.${system}.default}/bin/cx"; };
        docker-load = { type = "app"; program = "${self.packages.${system}.docker-load}/bin/docker-load"; };
        docker-push = { type = "app"; program = "${self.packages.${system}.docker-push}/bin/docker-push"; };
      });
      devShells = forEachSystem (system:
        let pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Tools
              uv
              python312
              git
              docker
              kubectl
              # Runtime Libraries needed by Python packages
              stdenv.cc.cc.lib
              zlib
              xz
              unixODBC
            ];
            shellHook = ''
              export PS1="\n\[\033[1;32m\][nix-cx-shell]\[\033[0m\] \[\033[1;34m\]\w\[030m\]\n\$ "
              echo "--- CX Shell Development Environment ---"
              if [ ! -d ".venv" ]; then uv venv; fi
              source .venv/bin/activate
              if [ ! -f ".venv/.installed" ]; then uv pip install -e '.[all]'; touch .venv/.installed; fi
              echo "✅ Ready to develop!"
            '';
          };
        }
      );
    };
}