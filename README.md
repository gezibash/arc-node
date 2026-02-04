# Arc Node

Arc Node is the node implementation for the Arc network. It ships as a single `arc` CLI binary and a Docker image.

## Install (macOS / Linux)

```sh
curl -fsSL https://raw.githubusercontent.com/gezibash/arc-node/master/install.sh | sh
```

### Options

- Install a specific version:

```sh
VERSION=v1.0.1 curl -fsSL https://raw.githubusercontent.com/gezibash/arc-node/master/install.sh | sh
```

- Install to a custom directory:

```sh
INSTALL_DIR="$HOME/bin" curl -fsSL https://raw.githubusercontent.com/gezibash/arc-node/master/install.sh | sh
```

The installer downloads the matching release asset, verifies the SHA256 checksum, and installs `arc` to `/usr/local/bin` (or `~/.local/bin` if not writable).

## Usage

```sh
arc version
arc node start --config /etc/arc/arc.hcl
```

## Docker

Images are published to:

- `ghcr.io/gezibash/arc`
- `docker.io/gezibash/arc`

Example:

```sh
docker pull ghcr.io/gezibash/arc:latest
```

## Releases

Release assets include platform binaries and `.sha256` checksum files.

## Repo layout

- `cmd/<binary>/`: executable entrypoints (`package main`).
- `internal/`: private implementation details (not importable outside this module).
- `pkg/`: public API surface for external consumers.
- `api/`: protobuf definitions and generated code.
- Root-level `arc`, `arc-blob`, `arc-relay`: expected build outputs for local workflows.
