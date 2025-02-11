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
      pkgs.llvmPackages_18.clang
    ];

    LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
    LIBCLANG_PATH = "${pkgs.llvmPackages_18.libclang.lib}/lib";
  }
