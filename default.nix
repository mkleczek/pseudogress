{ pkgs ? import <nixpkgs> { } }:
(pkgs.extend (import ./overlay.nix)).pseudogress
