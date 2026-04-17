#!/usr/bin/env bash
set -e

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

if [ ! -t 0 ]; then
    if command -v x-terminal-emulator >/dev/null 2>&1; then
        exec x-terminal-emulator -e "$APP_DIR/mydb.sh" "$@"
    fi

    if command -v gnome-terminal >/dev/null 2>&1; then
        exec gnome-terminal -- "$APP_DIR/mydb.sh" "$@"
    fi
fi

JAR="target/mydb-1.0-SNAPSHOT.jar"
DB="${1:-arquivo.bd}"

if [ ! -f "$JAR" ]; then
    mvn -q -DskipTests package
fi

exec java -jar "$JAR" "$DB"
