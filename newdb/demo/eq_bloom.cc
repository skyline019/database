#include "eq_bloom.h"

#include <cctype>
#include <fstream>
#include <sstream>

namespace {

std::uint64_t fnv1a64(const std::string& s) {
    std::uint64_t h = 1469598103934665603ull;
    for (unsigned char c : s) {
        h ^= static_cast<std::uint64_t>(c);
        h *= 1099511628211ull;
    }
    return h;
}

std::uint64_t mix64(std::uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

std::uint64_t h1(const std::string& s) {
    return mix64(std::hash<std::string>{}(s));
}

std::uint64_t h2(const std::string& s) {
    return mix64(fnv1a64(s));
}

void bloom_add(std::vector<std::uint8_t>& bits, const std::uint32_t m, const std::uint8_t k, const std::string& key) {
    const std::uint64_t a = h1(key);
    const std::uint64_t b = h2(key) | 1ull;
    for (std::uint8_t i = 0; i < k; ++i) {
        const std::uint64_t hv = a + static_cast<std::uint64_t>(i) * b;
        const std::uint32_t bit = static_cast<std::uint32_t>(hv % m);
        bits[bit / 8] |= static_cast<std::uint8_t>(1u << (bit % 8));
    }
}

bool bloom_may(const std::vector<std::uint8_t>& bits, const std::uint32_t m, const std::uint8_t k, const std::string& key) {
    const std::uint64_t a = h1(key);
    const std::uint64_t b = h2(key) | 1ull;
    for (std::uint8_t i = 0; i < k; ++i) {
        const std::uint64_t hv = a + static_cast<std::uint64_t>(i) * b;
        const std::uint32_t bit = static_cast<std::uint32_t>(hv % m);
        if ((bits[bit / 8] & static_cast<std::uint8_t>(1u << (bit % 8))) == 0) {
            return false;
        }
    }
    return true;
}

std::string hex_encode(const std::vector<std::uint8_t>& buf) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(buf.size() * 2);
    for (std::uint8_t b : buf) {
        out.push_back(kHex[(b >> 4) & 0xF]);
        out.push_back(kHex[b & 0xF]);
    }
    return out;
}

bool hex_decode(const std::string& s, std::vector<std::uint8_t>& out) {
    auto nyb = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
        return -1;
    };
    if ((s.size() % 2) != 0) return false;
    out.clear();
    out.reserve(s.size() / 2);
    for (std::size_t i = 0; i < s.size(); i += 2) {
        const int a = nyb(s[i]);
        const int b = nyb(s[i + 1]);
        if (a < 0 || b < 0) return false;
        out.push_back(static_cast<std::uint8_t>((a << 4) | b));
    }
    return true;
}

bool parse_hdr(const std::string& line, EqBloomHeader& out) {
    // v=1;data_sig=<n>;attr_sig=<n>;wal_lsn=<n>;bits=<n>;k=<n>
    if (line.rfind("v=1;", 0) != 0) return false;
    auto get = [&](const char* key) -> std::string {
        const std::string pat = std::string(key) + "=";
        const auto pos = line.find(pat);
        if (pos == std::string::npos) return {};
        auto end = line.find(';', pos);
        if (end == std::string::npos) end = line.size();
        return line.substr(pos + pat.size(), end - (pos + pat.size()));
    };
    try {
        out.data_sig = static_cast<std::uint64_t>(std::stoull(get("data_sig")));
        out.attr_sig = static_cast<std::uint64_t>(std::stoull(get("attr_sig")));
        out.wal_lsn = static_cast<std::uint64_t>(std::stoull(get("wal_lsn")));
        out.bits = static_cast<std::uint32_t>(std::stoul(get("bits")));
        out.k = static_cast<std::uint8_t>(std::stoul(get("k")));
    } catch (...) {
        return false;
    }
    return out.bits > 0 && out.k > 0;
}

} // namespace

bool eq_bloom_may_contain(const std::string& bloom_path,
                          const EqBloomHeader& expected,
                          const std::string& key,
                          bool& may_contain) {
    may_contain = true;
    std::ifstream in(bloom_path, std::ios::in);
    if (!in) return false;
    std::string hdr_line;
    std::string hex_line;
    if (!std::getline(in, hdr_line) || !std::getline(in, hex_line)) {
        return false;
    }
    EqBloomHeader got;
    if (!parse_hdr(hdr_line, got)) {
        return false;
    }
    if (got.data_sig != expected.data_sig || got.attr_sig != expected.attr_sig || got.wal_lsn != expected.wal_lsn ||
        got.bits != expected.bits || got.k != expected.k) {
        return false;
    }
    std::vector<std::uint8_t> bits;
    if (!hex_decode(hex_line, bits)) {
        return false;
    }
    const std::size_t want_bytes = (static_cast<std::size_t>(got.bits) + 7) / 8;
    if (bits.size() != want_bytes) {
        return false;
    }
    may_contain = bloom_may(bits, got.bits, got.k, key);
    return true;
}

void eq_bloom_write(const std::string& bloom_path,
                    const EqBloomHeader& hdr,
                    const std::vector<std::string>& keys) {
    const std::size_t bytes = (static_cast<std::size_t>(hdr.bits) + 7) / 8;
    std::vector<std::uint8_t> bits(bytes, 0);
    for (const auto& k : keys) {
        bloom_add(bits, hdr.bits, hdr.k, k);
    }
    std::ofstream out(bloom_path, std::ios::out | std::ios::trunc);
    if (!out) return;
    out << "v=1;data_sig=" << hdr.data_sig << ";attr_sig=" << hdr.attr_sig << ";wal_lsn=" << hdr.wal_lsn
        << ";bits=" << hdr.bits << ";k=" << static_cast<unsigned>(hdr.k) << "\n";
    out << hex_encode(bits) << "\n";
}

