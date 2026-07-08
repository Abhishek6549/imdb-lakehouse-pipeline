#!/usr/bin/env bash
#
# download_data.sh — fetch the raw IMDb title/rating/episode data into data/raw/
#
# The Kaggle mirror (ashirwadsangwan/imdb-dataset) requires a logged-in
# Kaggle account + API token, and is a straight repackaging of IMDb's own
# public "non-commercial datasets" (https://developer.imdb.com/non-commercial-datasets/).
# This script supports both sources:
#
#   --source kaggle   uses the `kaggle` CLI (needs ~/.kaggle/kaggle.json or
#                      KAGGLE_USERNAME/KAGGLE_KEY env vars) to pull the full
#                      2GB dataset used in the challenge brief.
#   --source imdb     (default) pulls the three files this pipeline actually
#                      needs directly from IMDb's own CDN. No login required,
#                      identical schema/content to the Kaggle mirror, and is
#                      what lets this repo be cloned and run end-to-end with
#                      zero credentials.
#
# Usage:
#   ./scripts/download_data.sh                 # IMDb direct download (default)
#   ./scripts/download_data.sh --source kaggle  # Kaggle API download

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RAW_DIR="${ROOT_DIR}/data/raw"
SOURCE="imdb"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$RAW_DIR"

download_from_kaggle() {
  if ! command -v kaggle >/dev/null 2>&1; then
    echo "The 'kaggle' CLI is not installed. Install it with: pip install kaggle" >&2
    exit 1
  fi
  echo "Downloading ashirwadsangwan/imdb-dataset from Kaggle into ${RAW_DIR} ..."
  kaggle datasets download -d ashirwadsangwan/imdb-dataset -p "$RAW_DIR" --unzip
  echo "Kaggle download complete."
}

download_from_imdb() {
  local base_url="https://datasets.imdbws.com"
  local files=("title.basics.tsv.gz" "title.ratings.tsv.gz" "title.episode.tsv.gz")

  for f in "${files[@]}"; do
    echo "Downloading ${f} ..."
    curl -sSL --fail -o "${RAW_DIR}/${f}" "${base_url}/${f}"
  done
  echo "IMDb direct download complete. Files saved to ${RAW_DIR}:"
  ls -lh "$RAW_DIR"
}

case "$SOURCE" in
  kaggle)
    download_from_kaggle
    ;;
  imdb)
    download_from_imdb
    ;;
  *)
    echo "Unknown --source '${SOURCE}'. Expected 'kaggle' or 'imdb'." >&2
    exit 1
    ;;
esac
