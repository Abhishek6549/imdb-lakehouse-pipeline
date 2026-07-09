#!/usr/bin/env bash
#
# download_data.sh — fetch the raw IMDb title/rating/episode data into data/raw/
#
# The Kaggle dataset (ashirwadsangwan/imdb-dataset, the ~2GB dataset named in
# the challenge brief) is a repackaging of IMDb's own public "non-commercial
# datasets" (https://developer.imdb.com/non-commercial-datasets/). It ships
# name.basics / title.akas / title.basics / title.principals / title.ratings
# — notably it does NOT include title.episode.tsv, so episode data is always
# pulled from IMDb directly regardless of --source.
#
#   --source kaggle   downloads the real ~1.8GB Kaggle zip via Kaggle's own
#                      dataset-download API endpoint (kaggle.com/api/v1/...).
#                      This works anonymously for this public dataset (no
#                      token needed); if Kaggle ever locks that endpoint down,
#                      it falls back to the `kaggle` CLI, which does need
#                      ~/.kaggle/kaggle.json or KAGGLE_USERNAME/KAGGLE_KEY.
#   --source imdb     (default) pulls all three files this pipeline needs
#                      directly from IMDb's own CDN. No login required,
#                      identical schema/content, and is what lets this repo
#                      be cloned and run end-to-end with zero credentials.
#
# Usage:
#   ./scripts/download_data.sh                 # IMDb direct download (default)
#   ./scripts/download_data.sh --source kaggle  # real Kaggle dataset download

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

download_episode_file_from_imdb() {
  # Kaggle's mirror doesn't ship title.episode.tsv, so this always runs,
  # regardless of --source, to fill that gap.
  if [[ -f "${RAW_DIR}/title.episode.tsv.gz" ]]; then
    return
  fi
  echo "Downloading title.episode.tsv.gz from IMDb directly (Kaggle's mirror doesn't include it) ..."
  curl -sSL --fail -o "${RAW_DIR}/title.episode.tsv.gz" "https://datasets.imdbws.com/title.episode.tsv.gz"
}

download_from_kaggle() {
  local zip_path="${RAW_DIR}/imdb-dataset.zip"
  local kaggle_url="https://www.kaggle.com/api/v1/datasets/download/ashirwadsangwan/imdb-dataset"

  echo "Downloading ashirwadsangwan/imdb-dataset from Kaggle (anonymous) ..."
  if curl -fsSL -o "$zip_path" "$kaggle_url"; then
    echo "Kaggle download complete (${zip_path})."
  else
    echo "Anonymous Kaggle download failed, falling back to the 'kaggle' CLI ..." >&2
    if ! command -v kaggle >/dev/null 2>&1; then
      echo "The 'kaggle' CLI is not installed. Install it with: pip install kaggle" >&2
      exit 1
    fi
    kaggle datasets download -d ashirwadsangwan/imdb-dataset -p "$RAW_DIR" -o
  fi

  echo "Extracting title.basics.tsv and title.ratings.tsv, compressing to .tsv.gz ..."
  unzip -p "$zip_path" title.basics.tsv | gzip > "${RAW_DIR}/title.basics.tsv.gz"
  unzip -p "$zip_path" title.ratings.tsv | gzip > "${RAW_DIR}/title.ratings.tsv.gz"
  rm -f "$zip_path"

  download_episode_file_from_imdb
  echo "Kaggle-sourced download complete. Files saved to ${RAW_DIR}:"
  ls -lh "$RAW_DIR"
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
