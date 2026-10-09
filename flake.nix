{
  description = "ochi logging database";

  inputs.nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-26.05";

  outputs = { nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;

      # build.zig.zon is the single version source; nixpkgs ships zig_X_Y and zls_X_Y
      # as a matched pair, so both come from the same attr suffix.
      zigVersion = lib.pipe ./build.zig.zon [
        builtins.readFile
        (builtins.match ".*\n[[:space:]]*\\.minimum_zig_version[[:space:]]*=[[:space:]]*\"([^\"]+)\".*")
        builtins.head
      ];
      suffix = lib.replaceStrings [ "." ] [ "_" ] (lib.versions.majorMinor zigVersion);
    in
    {
      devShells = lib.genAttrs [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ] (system:
        let pkgs = nixpkgs.legacyPackages.${system};
        in {
          default = pkgs.mkShell {
            packages = [ pkgs."zig_${suffix}" pkgs."zls_${suffix}" ];
          };
        });
    };
}
