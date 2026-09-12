#!/usr/bin/env bash
set -euo pipefail

sdk_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cmake -S "${sdk_dir}" -B "${sdk_dir}/build-asan" -DKRABKA_CPP_SANITIZERS=ON -DKRABKA_CPP_TSAN=OFF -DKRABKA_CPP_REQUIRE_EXTERNAL_DEPS=OFF
cmake --build "${sdk_dir}/build-asan"
ctest --test-dir "${sdk_dir}/build-asan" --output-on-failure

cmake -S "${sdk_dir}" -B "${sdk_dir}/build-tsan" -DKRABKA_CPP_SANITIZERS=OFF -DKRABKA_CPP_TSAN=ON -DKRABKA_CPP_REQUIRE_EXTERNAL_DEPS=OFF
cmake --build "${sdk_dir}/build-tsan" --target krabka_cpp_transport_test
ctest --test-dir "${sdk_dir}/build-tsan" -L tsan --output-on-failure
