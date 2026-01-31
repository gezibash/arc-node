#!/usr/bin/env sh
set -eu

REPO="gezibash/arc-node"
BIN_NAME="arc"
VERSION="${VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-}"

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
  fi
fi

mkdir -p "$INSTALL_DIR"

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Linux) os="linux";;
  Darwin) os="darwin";;
  *) echo "Unsupported OS: $os"; exit 1;;
 esac

case "$arch" in
  x86_64|amd64) arch="amd64";;
  arm64|aarch64) arch="arm64";;
  *) echo "Unsupported arch: $arch"; exit 1;;
 esac

asset="${BIN_NAME}_${os}_${arch}"

if [ "$VERSION" = "latest" ]; then
  base="https://github.com/${REPO}/releases/latest/download"
else
  case "$VERSION" in
    v*) ;; 
    *) VERSION="v${VERSION}";;
  esac
  base="https://github.com/${REPO}/releases/download/${VERSION}"
fi

workdir="$(mktemp -d)"
cleanup() { rm -rf "$workdir"; }
trap cleanup EXIT

fetch() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$out" "$url"
  else
    echo "curl or wget is required"; exit 1
  fi
}

fetch "${base}/${asset}" "$workdir/$asset"
fetch "${base}/${asset}.sha256" "$workdir/${asset}.sha256"

verify() {
  cd "$workdir"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${asset}.sha256"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -c "${asset}.sha256"
  else
    echo "No SHA256 verifier found (sha256sum/shasum)"; exit 1
  fi
}

verify

install_bin() {
  src="$1"
  dest="$2"
  if [ -w "$INSTALL_DIR" ]; then
    install -m 0755 "$src" "$dest"
  elif command -v sudo >/dev/null 2>&1; then
    sudo install -m 0755 "$src" "$dest"
  else
    echo "No write permission for $INSTALL_DIR and sudo not available"; exit 1
  fi
}

install_bin "$workdir/$asset" "$INSTALL_DIR/$BIN_NAME"

echo "Installed ${BIN_NAME} to ${INSTALL_DIR}/${BIN_NAME}"
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
  echo "Add ${INSTALL_DIR} to your PATH to use '${BIN_NAME}'"
fi
