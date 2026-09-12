#include "krabka/envelope.hpp"

#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::uint8_t hex_nibble(char value) {
  if (value >= '0' && value <= '9') return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f') return static_cast<std::uint8_t>(value - 'a' + 10);
  if (value >= 'A' && value <= 'F') return static_cast<std::uint8_t>(value - 'A' + 10);
  throw std::runtime_error("oracle vector contains a non-hex character");
}

std::vector<std::uint8_t> read_oracle_vector(const std::string& name) {
  std::ifstream input(std::string(KRABKA_CPP_VECTOR_DIR) + "/" + name);
  if (!input) throw std::runtime_error("missing oracle vector: " + name);

  std::string hex;
  for (char value = 0; input.get(value);) {
    if (std::isspace(static_cast<unsigned char>(value)) != 0) continue;
    hex.push_back(value);
  }
  if ((hex.size() % 2U) != 0U) throw std::runtime_error("oracle vector has an odd hex length: " + name);

  std::vector<std::uint8_t> bytes;
  bytes.reserve(hex.size() / 2U);
  for (std::size_t index = 0; index < hex.size(); index += 2U) {
    bytes.push_back(static_cast<std::uint8_t>((hex_nibble(hex[index]) << 4U) | hex_nibble(hex[index + 1U])));
  }
  return bytes;
}

} // namespace

int main() {
  const std::vector<std::uint8_t> payload{'p', 'a', 'y'};
  const auto encoded = krabka::envelope::encode(0, payload);
  assert(encoded == read_oracle_vector("message_payload_pay.hex"));
  const auto decoded = krabka::envelope::decode_one(encoded);
  assert(std::holds_alternative<krabka::envelope::Message>(decoded));
  assert(std::get<krabka::envelope::Message>(decoded).payload == payload);
  const auto compressed = krabka::envelope::decode_one(read_oracle_vector("compressed_payload_pay.hex"));
  assert(std::holds_alternative<krabka::envelope::Message>(compressed));
  assert(std::get<krabka::envelope::Message>(compressed).flags == 0x01);
  assert(std::holds_alternative<krabka::envelope::NeedMore>(krabka::envelope::decode_one(read_oracle_vector("partial_header.hex"))));
  const auto clean_end = krabka::envelope::decode_one(read_oracle_vector("endstream_clean.hex"));
  assert(std::holds_alternative<krabka::envelope::EndStream>(clean_end));
  assert(!std::get<krabka::envelope::EndStream>(clean_end).code.has_value());
  const auto end = krabka::envelope::decode_one(read_oracle_vector("endstream_not_found.hex"));
  assert(std::holds_alternative<krabka::envelope::EndStream>(end));
  assert(std::get<krabka::envelope::EndStream>(end).code == "not_found");
  const auto multi_frame = read_oracle_vector("multi_frame_stream.hex");
  const auto first_frame = krabka::envelope::decode_one(multi_frame);
  assert(std::holds_alternative<krabka::envelope::Message>(first_frame));
  assert(std::get<krabka::envelope::Message>(first_frame).payload == std::vector<std::uint8_t>({'o', 'n', 'e'}));
  const std::string error = R"({"error":{"code":"not_found","message":"missing"}})";
  const auto grpc_web_flagged = krabka::envelope::decode_one(krabka::envelope::encode(0x80, {error.begin(), error.end()}));
  assert(std::holds_alternative<krabka::envelope::Message>(grpc_web_flagged));
  return 0;
}
