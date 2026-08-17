#!/bin/bash

set -e

ENV_ROOT="${ENV_ROOT:-/var/cloudfunctions/runtimes}"
ALPINE_VERSION="3.19"
ALPINE_URL="https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/releases/x86_64/alpine-minirootfs-${ALPINE_VERSION}.0-x86_64.tar.gz"
ALPINE_TAR="/tmp/alpine-minirootfs.tar.gz"

log() {
    echo "[init] $1"
}

download_alpine() {
    if [ ! -f "$ALPINE_TAR" ]; then
        log "Downloading Alpine minirootfs..."
        curl -fsSL "$ALPINE_URL" -o "$ALPINE_TAR"
    else
        log "Alpine minirootfs already downloaded"
    fi
}

setup_base() {
    local name="$1"
    local dir="$ENV_ROOT/$name"

    if [ -d "$dir" ]; then
        log "Runtime '$name' already exists, skipping"
        return
    fi

    log "Setting up base for: $name"
    mkdir -p "$dir"
    tar -xf "$ALPINE_TAR" -C "$dir"
    echo "nameserver 1.1.1.1" > "$dir/etc/resolv.conf"
}

setup_python() {
    setup_base "python_env"
    local dir="$ENV_ROOT/python_env"
    log "Installing Python..."
    chroot "$dir" /bin/sh -c "apk update && apk add --no-cache python3"
    log "Python ready: $(chroot "$dir" python3 --version)"
}

setup_go() {
    setup_base "go_env"
    local dir="$ENV_ROOT/go_env"
    log "Installing Go..."
    chroot "$dir" /bin/sh -c "apk update && apk add --no-cache go"
    log "Go ready: $(chroot "$dir" go version)"
}

setup_java() {
    setup_base "java_env"
    local dir="$ENV_ROOT/java_env"
    log "Installing Java..."
    chroot "$dir" /bin/sh -c "apk update && apk add --no-cache openjdk21-jre"
    log "Java ready: $(chroot "$dir" java --version)"
}

main() {
    log "Starting CloudFunction runtime initialization"
    log "ENV_ROOT=$ENV_ROOT"

    mkdir -p "$ENV_ROOT"
    mkdir -p /var/cloudfunctions/functions

    download_alpine
    setup_python
    setup_go
    setup_java

    log "All runtimes ready"
}

main