#!/usr/bin/env sh
# -e -- exit if any command fails
# -u -- missing variable is invalid
set -eu

version="${1:-${version:-}}"
os="${2:-${os:-}}"
arch="${3:-${arch:-}}"

if [ -z "$version" ] || [ -z "$os" ] || [ -z "$arch" ]; then
    echo "usage: install.sh <version> <os> <arch>" >&2
    echo "or set version, os, and arch variables" >&2
    exit 2
fi

archive_name="Ochi-${version}-${os}-${arch}.tar.gz"
url="https://github.com/ochi-team/ochi/releases/download/${version}/${archive_name}"
install_root="$(pwd -P)"
install_dir="${install_root}/ochi-${version}-${os}-${arch}"
archive_path="${install_dir}/${archive_name}"

mkdir -p "$install_dir"
curl -fsSL "$url" -o "$archive_path"
tar -xzf "$archive_path" -C "$install_dir"

binary_path="$install_dir/Ochi"
if [ ! -f "$binary_path" ]; then
    echo "Ochi binary not found in ${archive_name}" >&2
    exit 1
fi

mv "$binary_path" "${install_root}/Ochi"
chmod +x "${install_root}/Ochi"
printf '%s\n' "${install_root}/Ochi"
