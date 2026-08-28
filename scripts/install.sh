#!/usr/bin/env bash
# Retracer installer. Gets an operator from zero to a running `retracerd`
# under systemd — no source checkout, no Rust toolchain, no Docker image.
#
#   curl -fsSL https://raw.githubusercontent.com/Arxium-Protocol/retracer/main/scripts/install.sh | bash
#
# read first (recommended, point one file):
#   curl -fsSL .../install.sh -o install.sh && less install.sh && bash install.sh
#
# Postgres is bring-your-own, same as scripts/setup.sh for a source build —
# this installer lays out the binary and config, it does not run a database
# for you.
set -euo pipefail

REPO="Arxium-Protocol/retracer"
ASSET_ARCH="x86_64-linux-gnu"

version=""
base_path="${RETRACERD_BASE_PATH:-$HOME/.retracer}"
assume_yes=0
dry_run=0

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

  --version vX.Y.Z   Install this release instead of latest.
  --base-path DIR    Install directory (default: ~/.retracer).
  --dry-run          Print what would happen; touch nothing.
  --yes              Non-interactive: accept every default, no prompts.
  -h, --help         This text.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="${2:?--version needs a tag, e.g. v0.1.0}"; shift 2 ;;
    --base-path) base_path="${2:?--base-path needs a directory}"; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --yes|-y) assume_yes=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'error: unknown argument: %s\n' "$1" >&2; usage >&2; exit 1 ;;
  esac
done

say() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33mwarning:\033[0m %s\n' "$1" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; exit 1; }
run() { if [ "$dry_run" -eq 1 ]; then printf '  would run: %s\n' "$*"; else "$@"; fi; }

# Falls back to the default when non-interactive (--yes, or no TTY on
# stdin — the `curl | bash` case).
ask() {
  local prompt="$1" default="$2" reply
  if [ "$assume_yes" -eq 1 ] || [ ! -t 0 ]; then
    printf '%s\n' "$default"
    return
  fi
  read -r -p "$prompt [$default]: " reply </dev/tty || reply=""
  printf '%s\n' "${reply:-$default}"
}

ask_yn() {
  local prompt="$1" default="$2" reply
  reply="$(ask "$prompt (y/n)" "$default")"
  case "$reply" in y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}

# ---------------------------------------------------------------- preflight

for tool in curl tar install; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done

# sha256sum on Linux, shasum -a 256 on macOS. Checksum verification is not
# optional — every downloaded asset gets checked against the release manifest.
if command -v sha256sum >/dev/null 2>&1; then
  sha256_file() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_file() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  die "need sha256sum or shasum to verify the download; refusing to install unverified"
fi

verify_checksum() {
  local sums_file=$1 filename=$2 expected actual
  expected=$(awk -v filename="$filename" \
    '$2 == filename || $2 == "*" filename { print $1 }' "$sums_file")
  printf '%s\n' "$expected" | grep -Eq '^[0-9a-fA-F]{64}$' \
    || die "SHA256SUMS has no single valid checksum for ${filename}"
  actual="$(sha256_file "$filename")"
  [ "$actual" = "$expected" ]
}

os="$(uname -s)"
arch="$(uname -m)"

# Releases only ship x86_64 Linux today (see .github/workflows/release.yml).
if [ "$os" != "Linux" ] || [ "$arch" != "x86_64" ]; then
  die "no prebuilt binary for ${os}/${arch} — releases are ${ASSET_ARCH} only.
Build from source instead: cargo build --release -p retracerd"
fi

# ------------------------------------------------------------ resolve version

if [ -z "$version" ]; then
  say "Resolving latest release..."
  # Unauthenticated API, 60 requests/hour/IP — plenty for an installer,
  # avoids depending on jq being present.
  version="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
  [ -n "$version" ] || die "could not determine the latest release tag.
Pass --version vX.Y.Z to skip the lookup entirely."
fi

asset="retracerd-${version}-${ASSET_ARCH}.tar.gz"
base_url="https://github.com/${REPO}/releases/download/${version}"
say "Installing retracerd ${version}"

# ------------------------------------------------------------------- prompts

base_path="$(ask 'Install directory' "$base_path")"
case "$base_path" in "~"*) base_path="${HOME}${base_path#\~}" ;; esac

bootnodes="$(ask 'Bootnode multiaddrs (comma-separated, blank = none)' '')"
# Strip an accidentally pasted "--bootnodes " / "--bootnodes=" prefix — the
# CLI help text prints the flag right next to the value, so pasting the
# whole thing here is an easy mistake and silently breaks multiaddr parsing
# at startup otherwise.
bootnodes="${bootnodes#--bootnodes=}"
bootnodes="${bootnodes#--bootnodes}"
bootnodes="${bootnodes# }"
database_url="$(ask 'Postgres URL' 'postgres://retracer:retracer@localhost:5433/retracer')"
node_rpc_url="$(ask "Chain node RPC URL (blank disables validator-uptime endpoint)" '')"
auth_token="$(ask 'Auth token (blank = HTTP/gRPC surfaces stay open, generate one with: openssl rand -hex 32)' '')"

if [ -z "$auth_token" ] && [ "$assume_yes" -ne 1 ]; then
  warn "no auth token set — HTTP and gRPC surfaces will accept requests from anyone who can reach them."
fi

# --------------------------------------------------------- download + verify

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

say "Downloading ${asset}"
if [ "$dry_run" -eq 1 ]; then
  printf '  download: %s/%s\n' "$base_url" "$asset"
  printf '  download: %s/SHA256SUMS\n' "$base_url"
  printf '  verify archive against SHA256SUMS before unpacking\n'
else
  # A 404 here is ambiguous between "private repo" and "no such tag" —
  # GitHub returns the same status for both, so name only one and point
  # the operator at the release list to check.
  curl -fSL --progress-bar -o "$tmp/$asset" "$base_url/$asset" \
    || die "could not download ${asset}.
Check: is ${version} a published release tag?
Releases: https://github.com/${REPO}/releases"
  curl -fsSL -o "$tmp/SHA256SUMS" "$base_url/SHA256SUMS" \
    || die "release ${version} has no SHA256SUMS asset; refusing to install unverified."

  say "Verifying checksum"
  ( cd "$tmp" && verify_checksum SHA256SUMS "$asset" ) \
    || die "checksum mismatch — do not run this binary"

  tar -xzf "$tmp/$asset" -C "$tmp"
fi

# --------------------------------------------------------------- lay out dirs

say "Creating ${base_path}"
for dir in bin configs; do
  run mkdir -p "$base_path/$dir"
done
run chmod 700 "$base_path"

extracted_dir="$tmp/retracerd-${version}-${ASSET_ARCH}"

say "Installing binary to ${base_path}/bin/retracerd"
if [ "$dry_run" -eq 1 ]; then
  printf '  install unpacked retracerd to %s/bin/retracerd\n' "$base_path"
  printf '  install kind_schema.toml to %s/configs/kind_schema.toml\n' "$base_path"
else
  install -m 0755 "$extracted_dir/retracerd" "$base_path/bin/retracerd"
  install -m 0644 "$extracted_dir/kind_schema.toml" "$base_path/configs/kind_schema.toml"
fi

# ------------------------------------------------------------------ env file

env_file="$base_path/configs/retracerd.env"
if [ -f "$env_file" ]; then
  warn "${env_file} already exists — keeping it. Delete and re-run to regenerate."
elif [ "$dry_run" -eq 1 ]; then
  printf '  would write %s\n' "$env_file"
else
  cat > "$env_file" <<ENVFILE
# retracerd configuration. Read by systemd (EnvironmentFile=) and by
# retracerd itself (clap \`env\` on its CLI args). A command-line flag
# always overrides these, so you can test a change one-off before editing
# this file.
#
# Apply changes with: systemctl restart retracerd

# Comma-separated peer multiaddrs to dial on startup.
RETRACER_BOOTNODES=$bootnodes

# Bring your own — retracerd does not run Postgres for you.
RETRACER_DATABASE_URL=$database_url

# This chain's node HTTP RPC. Only used for the validator-uptime endpoint;
# leave blank to disable it.
RETRACER_NODE_RPC_URL=$node_rpc_url

# Shared secret required as "Authorization: Bearer <token>" on the HTTP and
# gRPC surfaces (/health stays open either way). Blank means both surfaces
# stay open to anyone who can reach them.
RETRACER_AUTH_TOKEN=$auth_token

# Per-IP requests/second on both surfaces. Blank disables rate limiting.
RETRACER_RATE_LIMIT_RPS=
ENVFILE
  chmod 600 "$env_file"
fi

# ---------------------------------------------------------------- run it now

service_file="/etc/systemd/system/retracerd.service"
service_installed=0

if [ "$(uname -s)" != "Linux" ] || ! command -v systemctl >/dev/null 2>&1; then
  say "no systemd here — skipping service installation."
  echo "  Run in the foreground with:"
  echo "    set -a; . $env_file; set +a; $base_path/bin/retracerd --kind-schema $base_path/configs/kind_schema.toml"
else
  unit="$tmp/retracerd.service"
  cat > "$unit" <<UNIT
[Unit]
Description=Retracer indexer (retracerd)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$(id -un)
WorkingDirectory=$base_path
EnvironmentFile=$env_file
ExecStart=$base_path/bin/retracerd --kind-schema $base_path/configs/kind_schema.toml
Restart=always
RestartSec=5

# Hardening. retracerd needs its base path and the network, nothing else.
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$base_path

[Install]
WantedBy=multi-user.target
UNIT

  if [ "$dry_run" -eq 0 ]; then
    command -v sudo >/dev/null 2>&1 || die "installing the systemd service needs sudo"
  fi

  if ask_yn "Install systemd service (needs sudo, writes ${service_file})?" 'y'; then
    say "Installing ${service_file}"
    if [ "$dry_run" -eq 1 ]; then
      printf '  would sudo install -m 0644 unit %s\n' "$service_file"
      printf '  would run: sudo systemctl daemon-reload\n'
    else
      sudo install -m 0644 "$unit" "$service_file"
      sudo systemctl daemon-reload
    fi
    service_installed=1

    if ask_yn 'Start retracerd now and enable on boot?' 'y'; then
      run sudo systemctl enable --now retracerd
    fi
  else
    echo "  Unit written to $unit (removed on exit). Install later with:"
    echo "    sudo install -m 0644 <unit> $service_file && sudo systemctl daemon-reload"
    echo "  Or run in the foreground now:"
    echo "    set -a; . $env_file; set +a; $base_path/bin/retracerd --kind-schema $base_path/configs/kind_schema.toml"
  fi
fi

# ---------------------------------------------------------------------- done

cat <<DONE

$(say 'Done.')

  Binary   $base_path/bin/retracerd
  Config   $env_file
  Schema   $base_path/configs/kind_schema.toml

$(if [ "$service_installed" -eq 1 ]; then cat <<'SYSTEMD'
  Logs     journalctl -u retracerd -f
  Status   systemctl status retracerd
  Restart  systemctl restart retracerd
SYSTEMD
else
  printf '  Service not installed — see above for how to start it.\n'
fi)
  Health   curl -s localhost:8080/health
DONE
