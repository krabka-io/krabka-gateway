#!/usr/bin/env bash
# Checks the Go SDK compose harness without starting a container.
#
# The check reads the model that `docker compose config` resolves, with the
# default images, and fails when:
#
# - the compose file does not resolve
# - an image uses the `:edge` tag, which no workflow publishes
# - a health check uses `CMD-SHELL`, which needs a shell that the images do
#   not have
# - the broker image is not pinned by digest
# - the gateway image is not the tag that //packaging:image_load loads
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_CONFIG="${ROOT}/sdks/go/testdata/docker-compose.yml"
IMAGE_BUILD="${ROOT}/packaging/BUILD.bazel"

fail() {
  printf 'sdk-go harness: %s\n' "$*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 ||
  fail 'docker compose is not installed'
command -v jq >/dev/null 2>&1 || fail 'jq is not installed'

# Resolve with the defaults, not with overrides from the calling shell.
model="$(
  env -u KRABKA_BROKER_IMAGE -u KRABKA_GATEWAY_IMAGE \
    docker compose -f "${COMPOSE_CONFIG}" config --format json
)" || fail "docker compose config failed for ${COMPOSE_CONFIG}"

edge="$(jq -r '.services | to_entries[] | select(.value.image | test(":edge$")) | .key' <<<"${model}")"
[[ -z "${edge}" ]] || fail "services use an unpublished :edge image: ${edge//$'\n'/ }"

shell_checks="$(jq -r '.services | to_entries[] | select(.value.healthcheck.test[0]? == "CMD-SHELL") | .key' <<<"${model}")"
[[ -z "${shell_checks}" ]] || fail "services use a CMD-SHELL health check: ${shell_checks//$'\n'/ }"

broker_image="$(jq -r '.services.broker.image' <<<"${model}")"
[[ "${broker_image}" =~ ^ghcr\.io/krabka-io/krabka-broker@sha256:[0-9a-f]{64}$ ]] ||
  fail "the broker image is not pinned by digest: ${broker_image}"

format_image="$(jq -r '.services["broker-format"].image' <<<"${model}")"
[[ "${format_image}" == "${broker_image}" ]] ||
  fail "broker-format runs ${format_image}, but broker runs ${broker_image}"

loaded_tag="$(sed -n 's/^IMAGE_TAG = "\(.*\)"$/\1/p' "${IMAGE_BUILD}")"
[[ -n "${loaded_tag}" ]] || fail "no IMAGE_TAG in ${IMAGE_BUILD}"
gateway_image="$(jq -r '.services.gateway.image' <<<"${model}")"
[[ "${gateway_image}" == "${loaded_tag}" ]] ||
  fail "the gateway image is ${gateway_image}, but //packaging:image_load loads ${loaded_tag}"

printf 'sdk-go harness: compose resolves; broker %s; gateway %s\n' "${broker_image}" "${gateway_image}"
