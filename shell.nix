{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  packages = [
    pkgs.cargo
    pkgs.rustc
    pkgs.rustfmt
    pkgs.clippy
    # initdb, postgres, psql and pgbench, for recording and load tests
    pkgs.postgresql
  ];
}
