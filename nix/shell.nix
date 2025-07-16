{pkgs ? import <nixpkgs> {}}: let
  inherit (pkgs) lib;
in
  pkgs.mkShell rec {
    packages =
      [
        pkgs.capnproto
        pkgs.rust-analyzer
      ]
      ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [
        pkgs.mold
      ];

    buildInputs = [
      pkgs.llvmPackages_18.clang
      pkgs.openssl
      pkgs.rustToolchain
    ];

    LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
    LIBCLANG_PATH = "${pkgs.llvmPackages_18.libclang.lib}/lib";

    OPENSSL_DIR = "${pkgs.openssl.dev}";
    OPENSSL_LIB_DIR = "${pkgs.lib.getLib pkgs.openssl}/lib";
    OPENSSL_NO_VENDOR = 1;

    RUSTFLAGS =
      if pkgs.stdenv.hostPlatform.isLinux
      then "-C link-arg=-fuse-ld=mold"
      else "";

    PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
  }
