#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace krabka {

[[nodiscard]] std::string base64_encode(const std::vector<std::uint8_t>& bytes);
[[nodiscard]] std::vector<std::uint8_t> base64_decode(const std::string& text);

} // namespace krabka
