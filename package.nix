# Called by overlay.nix, which default.nix and flake.nix use. Other setups can
# call it directly on a fetched source: `callPackage "${src}/package.nix" { }`.
{ lib, rustPlatform }:
rustPlatform.buildRustPackage {
  pname = "pseudogress";
  version = (lib.importTOML ./Cargo.toml).package.version;
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./Cargo.toml
      ./Cargo.lock
      ./README.md
      ./src
    ];
  };
  cargoLock.lockFile = ./Cargo.lock;
  # The tests start PostgreSQL clusters and open thousands of connections;
  # they run in the nix shell instead of the build sandbox.
  doCheck = false;
  meta = {
    description = "Simulates many virtual PostgreSQL databases for load tests";
    license = lib.licenses.agpl3Only;
    mainProgram = "pseudogress";
  };
}
