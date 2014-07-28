#!/bin/sh
base=$(dirname "$0")
base=$(readlink -e "$base")
watch -n 5 -- bash $base/build.sh
