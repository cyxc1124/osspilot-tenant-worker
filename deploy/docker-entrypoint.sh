#!/bin/sh
set -eu
cmd=${1:-worker}
if [ "$#" -gt 0 ]; then
  shift
fi
case "$cmd" in
  worker) exec /app/worker "$@" ;;
  *) exec "$cmd" "$@" ;;
esac
