//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {};

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> lg(latch_);
    if (list_.size() == 0) {
        return false;
    }
    *frame_id = list_.front();
    list_.pop_front();
    cur_size_--;

    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(latch_);
    if (list_.size() == 0) {
        return;
    }
    auto it = list_.begin();
    for (; it != list_.end(); it++) {
        if (*it == frame_id) {
            list_.erase(it);
            cur_size_--;
            return;
        }
    }
    return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(latch_);
    auto it = list_.begin();
    for (; it != list_.end(); it++) {
        if (*it == frame_id) {
            return;
        }
    }
    if (list_.size() >= num_pages_) {
        list_.pop_front();
    }
    list_.push_back(frame_id);
    cur_size_++;
    return;
}

void LRUReplacer::Access(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(latch_);
    auto it = list_.begin();
    for (; it != list_.end(); it++) {
        if (*it == frame_id) {
            list_.erase(it);
            list_.push_back(frame_id);
            return;
        }
    }
    return;
}

auto LRUReplacer::Size() -> size_t { 
    return cur_size_;
}

}  // namespace bustub
