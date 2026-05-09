#pragma once

#include <cstdint>

namespace newdb {

enum class WalSyncMode : std::uint8_t {
    Full = 0,
    Normal = 1,
    Off = 2,
};

}  // namespace newdb
