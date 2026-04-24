#pragma once

#include <string>
#include <utility>

namespace newdb {

// Small explicit status carrier (no hidden errno; no exceptions required).
struct Status {
    bool ok{true};
    std::string message;

    static Status Ok() { return Status{}; }
    static Status Fail(std::string msg) {
        Status s;
        s.ok = false;
        s.message = std::move(msg);
        return s;
    }
    explicit operator bool() const { return ok; }

    bool failed() const { return !ok; }
};

} // namespace newdb
