#!/usr/bin/env bash
# Pinned local CLI. v1.9.1 bundles Temporal Server v1.32.0 for SAA support.
# Official release: https://github.com/temporalio/cli/releases/tag/v1.9.1
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
version=1.9.1
install_dir="$root/.tools/temporal-$version"
binary="$install_dir/temporal"

if [[ "${1:-}" == --install ]]; then
  shift
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) platform=darwin_arm64; checksum=41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56 ;;
    Darwin-x86_64) platform=darwin_amd64; checksum=48cdd6c84c56e27ae8e4d9a47052b89289a9c552dee029b49dd312c6fb72873f ;;
    Linux-aarch64|Linux-arm64) platform=linux_arm64; checksum=6c57c352d52fc3df34412376fd9ba6f74b7e3ace8e426e6cba8600156d36a145 ;;
    Linux-x86_64) platform=linux_amd64; checksum=09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5 ;;
    *) echo 'Unsupported platform; use the official Temporal CLI v1.9.1 distribution.' >&2; exit 1 ;;
  esac
  # SHA-256 values are pinned from this release's official checksums.txt.
  mkdir -p "$install_dir"
  archive="temporal_cli_${version}_${platform}.tar.gz"
  download_dir=$(mktemp -d "$install_dir/download.XXXXXX")
  trap 'rm -rf "$download_dir"' EXIT
  curl --fail --location --retry 2 --output "$download_dir/$archive" \
    "https://github.com/temporalio/cli/releases/download/v$version/$archive"
  if command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$download_dir/$archive" | awk '{print $1}')
  elif command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$download_dir/$archive" | awk '{print $1}')
  else
    echo 'Install shasum or sha256sum to verify the official archive.' >&2
    exit 1
  fi
  if [[ "$actual" != "$checksum" ]]; then
    echo "Temporal CLI archive checksum mismatch: expected $checksum; received $actual" >&2
    exit 1
  fi
  tar -xzf "$download_dir/$archive" -C "$download_dir" temporal LICENSE
  chmod +x "$download_dir/temporal"
  mv "$download_dir/temporal" "$binary"
  mv "$download_dir/LICENSE" "$install_dir/LICENSE"
  rm -rf "$download_dir"
  trap - EXIT
  if [[ $# == 0 ]]; then
    exec "$binary" --version
  fi
fi

if [[ ! -x "$binary" ]]; then
  echo "Pinned Temporal CLI v$version is missing. Run: bash scripts/temporal-runtime.sh --install" >&2
  exit 1
fi
exec "$binary" "$@"
