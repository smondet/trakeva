#!/usr/bin/env bash

echo "codegen: $*"

lib_dir=_gen/lib_dispatch
mkdir -p $lib_dir

lib_file=$lib_dir/trakeva_of_uri.ml

cat <<EOBLOB > $lib_file
let x = 42
EOBLOB


