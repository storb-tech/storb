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
      pkgs.llvmPackages_18.clang
      pkgs.openssl
      pkgs.rocksdb
      pkgs.rustToolchain
    ];

    LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
    LIBCLANG_PATH = "${pkgs.llvmPackages_18.libclang.lib}/lib";

    OPENSSL_DIR = "${pkgs.openssl.dev}";
    OPENSSL_LIB_DIR = "${pkgs.lib.getLib pkgs.openssl}/lib";
    OPENSSL_NO_VENDOR = 1;

    PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";

    ROCKSDB_LIB_DIR = "${pkgs.rocksdb}/lib";
  }
