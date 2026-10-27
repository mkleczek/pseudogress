{
  description = "Pseudogress, a PostgreSQL simulator for load-testing PostgreSQL clients";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  # A thin wrapper: the packaging is in overlay.nix, package.nix and shell.nix.
  outputs = { self, nixpkgs }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in
    {
      overlays.default = import ./overlay.nix;

      # The overlay only adds a package, so it can take nixpkgs as both final
      # and prev, without importing nixpkgs again. `nix run` starts
      # meta.mainProgram of the default package.
      packages = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          ours = self.overlays.default pkgs pkgs;
        in
        ours // { default = ours.pseudogress; });

      devShells = forAllSystems (system: {
        default = import ./shell.nix { pkgs = nixpkgs.legacyPackages.${system}; };
      });

      # `nix flake check` builds the package. Its tests run in the dev shell.
      checks = forAllSystems (system: {
        package = self.packages.${system}.pseudogress;
      });
    };
}
