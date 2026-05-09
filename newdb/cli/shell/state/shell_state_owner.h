#pragma once

#include <newdb/engine_session_opaque.h>

#include <memory>

class ShellState;

/// Owns a [`ShellState`](shell_state.h) in a single translation unit so call sites can avoid
/// `#include "cli/shell/state/shell_state.h"` when they only need `ShellState&` from `shell()`.
class ShellStateOwner {
public:
    ShellStateOwner();
    ~ShellStateOwner();
    ShellStateOwner(ShellStateOwner&&) noexcept;
    ShellStateOwner& operator=(ShellStateOwner&&) noexcept;
    ShellStateOwner(const ShellStateOwner&) = delete;
    ShellStateOwner& operator=(const ShellStateOwner&) = delete;

    [[nodiscard]] ShellState& shell() noexcept;
    [[nodiscard]] const ShellState& shell() const noexcept;

    /// Non-owning; `nullptr` when engine creation failed and shell owns `Session` directly.
    [[nodiscard]] newdb_engine_session_t* engine_host() const noexcept { return engine_; }

private:
    newdb_engine_session_t* engine_{nullptr};
    std::unique_ptr<ShellState> shell_;
};
