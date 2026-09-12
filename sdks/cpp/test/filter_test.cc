#include "krabka/client.hpp"
#include "krabka/errors.hpp"

#include <cassert>
#include <string>
#include <vector>

namespace {
std::vector<std::uint8_t> bytes(const std::string& value) { return {value.begin(), value.end()}; }
} // namespace

int main() {
  krabka::Client client;
  const auto publish_result = client.publish(krabka::Record{.topic = "events", .value = bytes(R"({"count":7,"active":true,"name":"seven"})"), .headers = {}});
  assert(publish_result.offset == 0);

  auto numeric_stream = client.subscribe({"events"}, "group", krabka::Filter{.path = "$.count", .op = "equals", .value = krabka::json::Value{7.0}});
  assert(numeric_stream.next(0).value == bytes(R"({"count":7,"active":true,"name":"seven"})"));

  auto boolean_stream = client.subscribe({"events"}, "group", krabka::Filter{.path = "$.active", .op = "equals", .value = krabka::json::Value{true}});
  assert(boolean_stream.next(0).value == bytes(R"({"count":7,"active":true,"name":"seven"})"));

  auto mismatched_stream = client.subscribe({"events"}, "group", krabka::Filter{.path = "$.count", .op = "equals", .value = krabka::json::Value{8.0}});
  try {
    (void)mismatched_stream.next(0);
    assert(false);
  } catch (const krabka::SdkError& error) {
    assert(error.error().kind == krabka::ErrorKind::NotFound);
  }

  return 0;
}
