#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

// Lightweight B+Tree-like index used for PAGE ordering.
// Implementation keeps ordered leaf chains semantics and supports duplicates.
class BPlusTreeIndex {
public:
    explicit BPlusTreeIndex(bool descending);

    void insert(std::string key, std::size_t slot);
    std::vector<std::size_t> flatten_slots() const;

private:
    struct Node {
        bool leaf{true};
        std::vector<std::string> keys;
        std::vector<std::unique_ptr<Node>> children;
        std::vector<std::vector<std::size_t>> values;
        Node* next{nullptr};
    };

    struct SplitResult {
        bool split{false};
        std::string promote_key;
        std::unique_ptr<Node> right;
    };

    static constexpr std::size_t kNodeFanout = 32;
    bool descending_{false};
    std::unique_ptr<Node> root_;

    SplitResult insert_recursive(Node* node, std::string key, std::size_t slot);
};
