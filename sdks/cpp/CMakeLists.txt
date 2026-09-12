cmake_minimum_required(VERSION 3.22)
project(krabka_sdk_cpp VERSION 0.1.0 LANGUAGES CXX)

option(KRABKA_CPP_SANITIZERS "Enable ASan/UBSan" ON)
option(KRABKA_CPP_TSAN "Enable TSan instead of ASan/UBSan" OFF)
option(KRABKA_CPP_REQUIRE_EXTERNAL_DEPS "Fail configure when libnghttp2 or protobuf are missing" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(KRABKA_CPP_TSAN AND KRABKA_CPP_SANITIZERS)
  message(FATAL_ERROR "KRABKA_CPP_TSAN and KRABKA_CPP_SANITIZERS are mutually exclusive")
endif()

if(KRABKA_CPP_TSAN AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(KRABKA_CPP_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(NGHTTP2 IMPORTED_TARGET libnghttp2)
find_package(Protobuf QUIET)

if(KRABKA_CPP_REQUIRE_EXTERNAL_DEPS AND (NOT NGHTTP2_FOUND OR NOT Protobuf_FOUND))
  message(FATAL_ERROR "full C++ SDK gate requires libnghttp2 and protobuf development packages")
endif()

add_library(krabka_cpp
  src/base64.cc
  src/client.cc
  src/envelope.cc
  src/errors.cc
  src/json.cc
  src/stubs.cc
  src/transport_nghttp2.cc
)
target_include_directories(krabka_cpp PUBLIC include)
if(NGHTTP2_FOUND)
  target_link_libraries(krabka_cpp PUBLIC PkgConfig::NGHTTP2)
  target_compile_definitions(krabka_cpp PRIVATE KRABKA_CPP_HAS_NGHTTP2)
else()
  message(WARNING "libnghttp2 not found; live transport will return Transport errors")
endif()
if(NOT Protobuf_FOUND)
  message(WARNING "protobuf not found; generated gateway bindings are not part of this local gate")
endif()
target_compile_options(krabka_cpp PRIVATE -Wall -Wextra -Wpedantic)

add_executable(krabka_cpp_adapter adapter/main.cc)
target_link_libraries(krabka_cpp_adapter PRIVATE krabka_cpp)

enable_testing()
add_executable(krabka_cpp_codec_test test/codec_test.cc)
target_link_libraries(krabka_cpp_codec_test PRIVATE krabka_cpp)
target_compile_definitions(krabka_cpp_codec_test PRIVATE KRABKA_CPP_VECTOR_DIR="${CMAKE_CURRENT_SOURCE_DIR}/test/vectors")
add_test(NAME krabka_cpp_codec_test COMMAND krabka_cpp_codec_test)
add_executable(krabka_cpp_filter_test test/filter_test.cc)
target_link_libraries(krabka_cpp_filter_test PRIVATE krabka_cpp)
add_test(NAME krabka_cpp_filter_test COMMAND krabka_cpp_filter_test)
add_executable(krabka_cpp_transport_test test/transport_test.cc)
target_link_libraries(krabka_cpp_transport_test PRIVATE krabka_cpp)
target_include_directories(krabka_cpp_transport_test PRIVATE src)
add_test(NAME krabka_cpp_transport_test COMMAND krabka_cpp_transport_test)
set_tests_properties(krabka_cpp_transport_test PROPERTIES LABELS "transport;tsan")
add_executable(krabka_cpp_queue_test test/queue_test.cc)
target_link_libraries(krabka_cpp_queue_test PRIVATE krabka_cpp)
add_test(NAME krabka_cpp_queue_test COMMAND krabka_cpp_queue_test)
