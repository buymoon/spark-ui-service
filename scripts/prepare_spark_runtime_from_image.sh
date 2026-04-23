#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-runtime}"
SPARK_VERSION="${SPARK_VERSION:-$(sed -n 's:.*<spark.version>\(.*\)</spark.version>.*:\1:p' "$ROOT_DIR/pom.xml" | head -n 1)}"
SPARK_IMAGE="${SPARK_IMAGE:-}"
SPARK_IMAGE_SPARK_HOME="${SPARK_IMAGE_SPARK_HOME:-/opt/spark}"
SPARK_IMAGE_JAVA_HOME="${SPARK_IMAGE_JAVA_HOME:-/opt/java/openjdk}"
SPARK_RUNTIME_ROOT="${SPARK_RUNTIME_ROOT:-/tmp/spark-image-runtime}"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 1
  fi
}

detect_local_spark_image() {
  local image_list
  image_list="$(docker images --format '{{.Repository}}:{{.Tag}}' | grep '^apache/spark:' || true)"
  if [[ -z "$image_list" ]]; then
    return 1
  fi

  if [[ -n "$SPARK_VERSION" ]]; then
    local exact_match
    exact_match="$(printf '%s\n' "$image_list" | grep -E "^apache/spark:${SPARK_VERSION}([:-]|$)" | head -n 1 || true)"
    if [[ -n "$exact_match" ]]; then
      printf '%s\n' "$exact_match"
      return 0
    fi

    local major_minor="${SPARK_VERSION%.*}"
    local family_match
    family_match="$(printf '%s\n' "$image_list" | grep -E "^apache/spark:${major_minor}\\." | head -n 1 || true)"
    if [[ -n "$family_match" ]]; then
      printf '%s\n' "$family_match"
      return 0
    fi
  fi

  printf '%s\n' "$image_list" | head -n 1
}

prepare_runtime_from_image() {
  local image="$1"
  local image_key spark_home java_home container_id

  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Spark image $image is not present locally. Pull it first or set SPARK_IMAGE to an existing local image." >&2
    exit 1
  fi

  image_key="$(printf '%s' "$image" | tr '/:' '__')"
  spark_home="$SPARK_RUNTIME_ROOT/$image_key/spark"
  java_home="$SPARK_RUNTIME_ROOT/$image_key/java"

  if [[ ! -x "$spark_home/bin/spark-submit" || ! -x "$java_home/bin/java" ]]; then
    rm -rf "$spark_home" "$java_home"
    mkdir -p "$spark_home" "$java_home"
    container_id="$(docker create "$image")"
    trap 'docker rm -f "$container_id" >/dev/null 2>&1 || true' EXIT
    docker cp "$container_id:$SPARK_IMAGE_SPARK_HOME/." "$spark_home"
    docker cp "$container_id:$SPARK_IMAGE_JAVA_HOME/." "$java_home"
    docker rm -f "$container_id" >/dev/null 2>&1 || true
    trap - EXIT
  fi

  printf 'export SPARK_IMAGE=%q\n' "$image"
  printf 'export SPARK_HOME=%q\n' "$spark_home"
  printf 'export JAVA_HOME=%q\n' "$java_home"
}

require_command docker

if [[ -z "$SPARK_IMAGE" ]]; then
  SPARK_IMAGE="$(detect_local_spark_image || true)"
fi

if [[ -z "$SPARK_IMAGE" ]]; then
  echo "No local apache/spark image was found. Set SPARK_IMAGE to a local image tag first." >&2
  exit 1
fi

if [[ "$MODE" == "--image-only" ]]; then
  printf '%s\n' "$SPARK_IMAGE"
  exit 0
fi

prepare_runtime_from_image "$SPARK_IMAGE"
