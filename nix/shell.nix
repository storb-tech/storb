{pkgs ? import <nixpkgs> {}}: let
  inherit (pkgs) lib;
in
  pkgs.mkShell rec {
    packages =
      [
        # pkgs.capnproto
        pkgs.rust-analyzer
      ]
      ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [
        pkgs.mold
      ];

    buildInputs = [
      pkgs.rustToolchain
    ];

    LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
  }
