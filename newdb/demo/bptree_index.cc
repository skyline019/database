#include "bptree_index.h"

#include <algorithm>

BPlusTreeIndex::BPlusTreeIndex(const bool descending)
    : descending_(descending), root_(std::make_unique<Node>()) {}

BPlusTreeIndex::SplitResult BPlusTreeIndex::insert_recursive(Node* node, std::string key, const std::size_t slot) {
    if (node->leaf) {
        const auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        const std::size_t pos = static_cast<std::size_t>(it - node->keys.begin());
        if (it != node->keys.end() && *it == key) {
            node->values[pos].push_back(slot);
        } else {
            node->keys.insert(it, std::move(key));
            node->values.insert(node->values.begin() + static_cast<std::ptrdiff_t>(pos), std::vector<std::size_t>{slot});
        }

        if (node->keys.size() <= kNodeFanout) {
            return {};
        }
        auto right = std::make_unique<Node>();
        right->leaf = true;
        const std::size_t mid = node->keys.size() / 2;
        right->keys.assign(node->keys.begin() + static_cast<std::ptrdiff_t>(mid), node->keys.end());
        right->values.assign(node->values.begin() + static_cast<std::ptrdiff_t>(mid), node->values.end());
        node->keys.resize(mid);
        node->values.resize(mid);
        right->next = node->next;
        node->next = right.get();

        SplitResult res;
        res.split = true;
        res.promote_key = right->keys.front();
        res.right = std::move(right);
        return res;
    }

    const auto child_it = std::upper_bound(node->keys.begin(), node->keys.end(), key);
    const std::size_t child_idx = static_cast<std::size_t>(child_it - node->keys.begin());
    SplitResult child_split = insert_recursive(node->children[child_idx].get(), std::move(key), slot);
    if (!child_split.split) {
        return {};
    }
    node->keys.insert(node->keys.begin() + static_cast<std::ptrdiff_t>(child_idx), child_split.promote_key);
    node->children.insert(node->children.begin() + static_cast<std::ptrdiff_t>(child_idx + 1), std::move(child_split.right));
    if (node->keys.size() <= kNodeFanout) {
        return {};
    }

    auto right = std::make_unique<Node>();
    right->leaf = false;
    const std::size_t mid = node->keys.size() / 2;
    std::string promote = node->keys[mid];

    right->keys.assign(node->keys.begin() + static_cast<std::ptrdiff_t>(mid + 1), node->keys.end());
    right->children.assign(
        std::make_move_iterator(node->children.begin() + static_cast<std::ptrdiff_t>(mid + 1)),
        std::make_move_iterator(node->children.end()));

    node->keys.resize(mid);
    node->children.resize(mid + 1);

    SplitResult res;
    res.split = true;
    res.promote_key = std::move(promote);
    res.right = std::move(right);
    return res;
}

void BPlusTreeIndex::insert(std::string key, const std::size_t slot) {
    SplitResult split = insert_recursive(root_.get(), std::move(key), slot);
    if (!split.split) {
        return;
    }
    auto new_root = std::make_unique<Node>();
    new_root->leaf = false;
    new_root->keys.push_back(std::move(split.promote_key));
    new_root->children.push_back(std::move(root_));
    new_root->children.push_back(std::move(split.right));
    root_ = std::move(new_root);
}

std::vector<std::size_t> BPlusTreeIndex::flatten_slots() const {
    std::vector<std::size_t> out;
    const Node* cur = root_.get();
    if (cur == nullptr) {
        return out;
    }
    while (!cur->leaf) {
        cur = cur->children.front().get();
    }

    std::vector<std::size_t> asc;
    for (const Node* leaf = cur; leaf != nullptr; leaf = leaf->next) {
        for (const auto& bucket : leaf->values) {
            asc.insert(asc.end(), bucket.begin(), bucket.end());
        }
    }
    if (!descending_) {
        return asc;
    }
    out.assign(asc.rbegin(), asc.rend());
    return out;
}
