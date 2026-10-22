#!/bin/sh
# Writes the UI's runtime config. FASTER_API_URL empty = same origin
# (the API is proxied by this nginx).
set -eu
api="${FASTER_API_URL:-}"
case "$api" in
  *\"*|*\\*) echo "FASTER_API_URL must not contain quotes or backslashes" >&2; exit 1 ;;
esac
printf 'window.__FASTER__ = { apiUrl: "%s" };\n' "$api" > /usr/share/nginx/html/config.js
