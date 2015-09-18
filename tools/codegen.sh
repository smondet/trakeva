#!/usr/bin/env bash

sqlite=$1
postgresql=$2

set -e

echo "Codegen: SQLITE: $sqlite, POSTGRES: $postgresql"

lib_dir=gen/lib_of_uri
mkdir -p $lib_dir

lib_ml=$lib_dir/trakeva_of_uri.ml

cat <<EOBLOB > $lib_ml
(* This is generated code, see ./tools/codegen.sh *)
let x = 42
module S = Trakeva_sqlite
module PG = Trakeva_postgresql
EOBLOB


