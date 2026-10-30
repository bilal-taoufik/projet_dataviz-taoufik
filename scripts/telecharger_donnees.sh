#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p data

URL="https://www.data.gouv.fr/api/1/datasets/r/d7aed239-8dc1-46dd-bc66-3d18097cb7ea"

echo "Téléchargement des données décès..."

curl -fL --retry 3 -o data/deces.parquet "$URL"

echo "Téléchargement terminé !"
ls -lh data/deces.parquet
