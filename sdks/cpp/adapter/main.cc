#include "krabka/base64.hpp"
#include "krabka/client.hpp"
#include "krabka/json.hpp"

#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace {
using krabka::json::Array;
using krabka::json::Object;
using krabka::json::Value;

constexpr double kContractMajor = 1;
constexpr double kContractMinor = 1;
constexpr std::int64_t kMaxSafeJsonInteger = 9'007'199'254'740'991;

Value string_value(const std::string& value) { return Value{value}; }
Value null_value() { return Value{nullptr}; }
Value number_value(double value) { return Value{value}; }
Value bool_value(bool value) { return Value{value}; }
Value object_value(Object value) { return Value{std::move(value)}; }
Value array_value(Array value) { return Value{std::move(value)}; }

Object ok(Object body) { return Object{{"ok", object_value(std::move(body))}}; }

Object error_response(const krabka::Error& error) {
  Object body{{"kind", string_value(krabka::to_string(error.kind))}};
  if (!error.module.empty()) body.emplace("module", string_value(error.module));
  if (!error.gated_on.empty()) body.emplace("gated_on", string_value(error.gated_on));
  if (!error.message.empty()) body.emplace("message", string_value(error.message));
  return Object{{"error", object_value(std::move(body))}};
}

Value safe_integer_value(std::int64_t value, const std::string& field) {
  if (value < -kMaxSafeJsonInteger || value > kMaxSafeJsonInteger) {
    throw krabka::SdkError(krabka::error_with_message(
        krabka::ErrorKind::InvalidArgument,
        field + " cannot be represented exactly by the JSON-lines adapter"));
  }
  return number_value(static_cast<double>(value));
}

std::vector<krabka::Header> parse_headers(const Object& command) {
  std::vector<krabka::Header> headers;
  auto found = command.find("headers");
  if (found == command.end()) return headers;
  for (const auto& value : krabka::json::as_array(found->second)) {
    const auto& object = krabka::json::as_object(value);
    std::optional<std::vector<std::uint8_t>> header_value;
    if (krabka::json::has_non_null(object, "value_b64")) {
      header_value = krabka::base64_decode(krabka::json::get_string(object, "value_b64"));
    }
    headers.push_back({krabka::json::get_string(object, "name"), std::move(header_value)});
  }
  return headers;
}

Array headers_json(const std::vector<krabka::Header>& headers) {
  Array out;
  for (const auto& header : headers) {
    out.push_back(object_value(Object{{"name", string_value(header.name)}, {"value_b64", header.value.has_value() ? string_value(krabka::base64_encode(*header.value)) : null_value()}}));
  }
  return out;
}

std::optional<krabka::Filter> parse_filter(const Object& command) {
  auto found = command.find("filter");
  if (found == command.end() || std::holds_alternative<std::nullptr_t>(found->second.data)) return std::nullopt;
  const auto& object = krabka::json::as_object(found->second);
  return krabka::Filter{.path = krabka::json::get_string(object, "path"), .op = krabka::json::get_string(object, "op"), .value = object.at("value")};
}

std::vector<std::string> parse_topics(const Object& command) {
  std::vector<std::string> topics;
  for (const auto& topic : krabka::json::as_array(command.at("topics"))) topics.push_back(krabka::json::as_string(topic));
  return topics;
}

std::uint64_t uint64_value(const Object& command, const std::string& key) {
  const auto* number = std::get_if<double>(&command.at(key).data);
  if (number == nullptr || !std::isfinite(*number) || *number < 0 || std::trunc(*number) != *number ||
      *number >= 18'446'744'073'709'551'616.0) {
    throw krabka::SdkError(
        krabka::error_with_message(krabka::ErrorKind::InvalidArgument, key + " must be an unsigned integer"));
  }
  return static_cast<std::uint64_t>(*number);
}

std::uint32_t uint32_value(const Object& command, const std::string& key) {
  const auto value = uint64_value(command, key);
  if (value > std::numeric_limits<std::uint32_t>::max()) {
    throw krabka::SdkError(
        krabka::error_with_message(krabka::ErrorKind::InvalidArgument, key + " is too large"));
  }
  return static_cast<std::uint32_t>(value);
}

krabka::QueueAckType parse_ack_type(const std::string& value) {
  if (value == "accept") return krabka::QueueAckType::Accept;
  if (value == "release") return krabka::QueueAckType::Release;
  if (value == "reject") return krabka::QueueAckType::Reject;
  throw krabka::SdkError(krabka::error_with_message(
      krabka::ErrorKind::InvalidArgument, "unknown queue ack_type"));
}

std::vector<krabka::QueueAckEntry> parse_queue_ack_entries(const Object& command) {
  std::vector<krabka::QueueAckEntry> entries;
  for (const auto& value : krabka::json::as_array(command.at("entries"))) {
    const auto& object = krabka::json::as_object(value);
    entries.push_back(krabka::QueueAckEntry{
        .message_id = krabka::json::get_string(object, "message_id"),
        .ack_type = parse_ack_type(krabka::json::get_string(object, "ack_type"))});
  }
  return entries;
}

std::vector<krabka::QueueRenewEntry> parse_queue_renew_entries(const Object& command) {
  std::vector<krabka::QueueRenewEntry> entries;
  for (const auto& value : krabka::json::as_array(command.at("entries"))) {
    const auto& object = krabka::json::as_object(value);
    entries.push_back(
        krabka::QueueRenewEntry{.message_id = krabka::json::get_string(object, "message_id")});
  }
  return entries;
}

Object queue_acquire_response(const krabka::QueueAcquireResult& result,
                              const std::string& public_session_id) {
  Array messages;
  for (const auto& message : result.messages) {
    messages.push_back(object_value(Object{
        {"delivery_count", number_value(static_cast<double>(message.delivery_count))},
        {"headers", array_value(headers_json(message.headers))},
        {"message_id", string_value(message.message_id)},
        {"offset", safe_integer_value(message.offset, "offset")},
        {"partition", number_value(static_cast<double>(message.partition))},
        {"topic", string_value(message.topic)},
        {"value_b64", message.value.has_value()
                          ? string_value(krabka::base64_encode(*message.value))
                          : null_value()},
    }));
  }
  return ok(Object{{"messages", array_value(std::move(messages))},
                   {"session_id", string_value(public_session_id)}});
}

Object queue_batch_response(const krabka::QueueBatchResult& batch) {
  Array results;
  for (const auto& result : batch.results) {
    Value error = null_value();
    if (result.error.has_value()) {
      error = object_value(Object{
          {"kind", string_value(krabka::to_string(result.error->kind))},
          {"message", string_value(result.error->message)},
      });
    }
    results.push_back(object_value(Object{{"error", std::move(error)},
                                          {"message_id", string_value(result.message_id)}}));
  }
  return ok(Object{{"results", array_value(std::move(results))}});
}

Object publish_response(const krabka::PublishResult& result) {
  return ok(Object{{"deduplicated", bool_value(result.deduplicated)}, {"offset", safe_integer_value(result.offset, "offset")}, {"partition", number_value(static_cast<double>(result.partition))}});
}

class Adapter {
public:
  Object handle(const Object& command) {
    const std::string cmd = krabka::json::get_string(command, "cmd");
    try {
      if (cmd == "hello") {
        return Object{{"hello",
                       object_value(Object{{"contract_major", number_value(kContractMajor)},
                                           {"contract_minor", number_value(kContractMinor)},
                                           {"language", string_value("cpp")}})}};
      }
      if (cmd == "configure") {
        std::optional<std::string> bearer;
        if (krabka::json::has_non_null(command, "bearer")) bearer = krabka::json::get_string(command, "bearer");
        client_.configure(krabka::json::get_string(command, "endpoint"), bearer);
        stream_ = std::nullopt;
        queue_session_aliases_.clear();
        next_queue_session_id_ = 1;
        return ok(Object{{"bearer_configured", bool_value(client_.bearer_configured())}});
      }
      if (cmd == "publish") {
        auto value = krabka::base64_decode(krabka::json::get_string(command, "value_b64"));
        return publish_response(client_.publish(krabka::Record{.topic = krabka::json::get_string(command, "topic"), .value = std::move(value), .headers = parse_headers(command)}));
      }
      if (cmd == "publish_event") {
        const auto& event_object = krabka::json::as_object(command.at("event"));
        krabka::CloudEvent event{.id = krabka::json::get_string(event_object, "id"), .source = krabka::json::get_string(event_object, "source"), .type = krabka::json::get_string(event_object, "type"), .specversion = krabka::json::get_string(event_object, "specversion"), .datacontenttype = std::nullopt, .data = krabka::base64_decode(krabka::json::get_string(event_object, "data_b64"))};
        if (krabka::json::has_non_null(event_object, "datacontenttype")) event.datacontenttype = krabka::json::get_string(event_object, "datacontenttype");
        return publish_response(client_.publish_event(krabka::json::get_string(command, "topic"), std::move(event)));
      }
      if (cmd == "subscribe") {
        stream_ = client_.subscribe(parse_topics(command), krabka::json::get_string(command, "group"), parse_filter(command));
        return ok(Object{});
      }
      if (cmd == "next_message") {
        if (!stream_.has_value()) return error_response(krabka::error_with_message(krabka::ErrorKind::InvalidArgument, "subscribe before next_message"));
        auto message = stream_->next(static_cast<std::uint64_t>(std::get<double>(command.at("timeout_ms").data)));
        return Object{{"message", object_value(Object{{"headers", array_value(headers_json(message.headers))}, {"offset", safe_integer_value(message.offset, "offset")}, {"partition", number_value(static_cast<double>(message.partition))}, {"topic", string_value(message.topic)}, {"value_b64", string_value(krabka::base64_encode(message.value))}})}};
      }
      if (cmd == "queue_acquire") {
        const auto result = client_.queue_acquire(
            krabka::json::get_string(command, "topic"),
            krabka::json::get_string(command, "group"), uint32_value(command, "max"),
            uint64_value(command, "lock_duration_ms"),
            actual_queue_session(krabka::json::get_string(command, "session_id")));
        const auto public_session_id = remember_queue_session(result.session_id);
        return queue_acquire_response(result, public_session_id);
      }
      if (cmd == "queue_ack") return error_response(client_.queue_error());
      if (cmd == "queue_acknowledge") {
        return queue_batch_response(client_.queue_acknowledge(
            actual_queue_session(krabka::json::get_string(command, "session_id")),
            parse_queue_ack_entries(command)));
      }
      if (cmd == "queue_renew") {
        return queue_batch_response(client_.queue_renew(
            actual_queue_session(krabka::json::get_string(command, "session_id")),
            parse_queue_renew_entries(command)));
      }
      if (cmd == "db_connect") return error_response(client_.database_error());
      if (cmd == "auth_sign_in") return error_response(client_.auth_error());
      if (cmd == "blob_put" || cmd == "blob_get") return error_response(client_.blob_error());
      return error_response(krabka::error_with_message(krabka::ErrorKind::InvalidArgument, "unknown command"));
    } catch (const krabka::SdkError& error) {
      return error_response(error.error());
    }
  }

private:
  std::string remember_queue_session(const std::string& actual_session_id) {
    if (actual_session_id.empty()) return actual_session_id;
    for (const auto& [public_session_id, stored_actual_session_id] : queue_session_aliases_) {
      if (stored_actual_session_id == actual_session_id) return public_session_id;
    }
    const auto public_session_id = "queue-session-" + std::to_string(next_queue_session_id_++);
    queue_session_aliases_[public_session_id] = actual_session_id;
    return public_session_id;
  }

  std::string actual_queue_session(const std::string& public_session_id) const {
    const auto found = queue_session_aliases_.find(public_session_id);
    return found == queue_session_aliases_.end() ? public_session_id : found->second;
  }

  krabka::Client client_;
  std::optional<krabka::MessageStream> stream_;
  std::map<std::string, std::string> queue_session_aliases_;
  std::uint64_t next_queue_session_id_ = 1;
};
} // namespace

int main() {
  Adapter adapter;
  std::string line;
  while (std::getline(std::cin, line)) {
    const auto command = krabka::json::as_object(krabka::json::parse(line));
    std::cout << krabka::json::stringify(object_value(adapter.handle(command))) << '\n' << std::flush;
  }
  return 0;
}
