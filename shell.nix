{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
     nativeBuildInputs = [ pkgs.lmdb ];
}
