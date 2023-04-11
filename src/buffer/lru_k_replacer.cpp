//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : num_frames_(num_frames), k_(k) {
    new_cache_list_ = new LRUReplacer(num_frames);
    old_cache_list_ = new LRUReplacer(num_frames);
}

LRUKReplacer::~LRUKReplacer() {
    delete new_cache_list_;
    delete old_cache_list_;
}

// evict一个evictable的frame。成功返回true, 并且赋值给frame_id. 如果没有可以evict的frame返回false
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> lg(latch_);
    if (old_cache_list_->Size() + new_cache_list_->Size() == 0) {
        return false;
    }
    
    if (new_cache_list_->Size() != 0) {
        bool flag = new_cache_list_->Victim(frame_id);
        if (flag) {
            curr_size_--;
            map_.erase(*frame_id);
            return flag;
        }
    }
    
    if (old_cache_list_->Size() != 0) {
        bool flag = old_cache_list_->Victim(frame_id);
        if (flag) {
            curr_size_--;
            map_.erase(*frame_id);
            return flag;
        }
    }
    return false;
}

// 在当前的timestamp记录当前的frame_id，通常在page pinned之后调用
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> lg(latch_);
    // map 中的 evictable 状态
    auto it = map_.find(frame_id);
    if (it == map_.end()) {
        current_timestamp_++;
        map_[frame_id] = new LRUKNode(frame_id, k_);
        map_[frame_id]->add_history(current_timestamp_);
    } else {
        current_timestamp_++;
        if (map_[frame_id]->is_evictable_) {
            if (map_[frame_id]->is_old_cache()) {
                map_[frame_id]->add_history(current_timestamp_);
                old_cache_list_->Access(frame_id);
            } else {
                map_[frame_id]->add_history(current_timestamp_);
                if (map_[frame_id]->is_old_cache()) {
                    new_cache_list_->Pin(frame_id);
                    old_cache_list_->Unpin(frame_id);
                } else {
                    new_cache_list_->Access(frame_id);
                }
            }
        } else {
            map_[frame_id]->add_history(current_timestamp_);
        }
    }

    return;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lg(latch_);
    auto it = map_.find(frame_id);
    if (it == map_.end() && set_evictable) {
        if (curr_size_ >= num_frames_) {
            frame_id_t tmp;
            if (new_cache_list_->Size() > 0) {
                if (!new_cache_list_->Victim(&tmp)) {
                    throw ExecutionException("new size not zero, but can't victim");
                }
            } else {
                if (!old_cache_list_->Victim(&tmp)) {
                    throw ExecutionException("old size not zero, but can't victim");
                }
            }
            auto it_tmp = map_.find(tmp);
            if (it_tmp == map_.end()) {
                throw ExecutionException("map can't find victim");
            }
            map_.erase(it_tmp);
        } else {
            curr_size_++;
        }
        map_[frame_id] = new LRUKNode(frame_id, k_);
        new_cache_list_->Unpin(frame_id);
        return;
    } else if (it == map_.end()) {
        return;
    }

    if (set_evictable) {
        if (!it->second->is_evictable_) {
            it->second->is_evictable_ = true;
            if (curr_size_ >= num_frames_) {
                frame_id_t tmp;
                if (new_cache_list_->Size() > 0) {
                    if (!new_cache_list_->Victim(&tmp)) {
                        throw ExecutionException("new size not zero, but can't victim");
                    }
                } else {
                    if (!old_cache_list_->Victim(&tmp)) {
                        throw ExecutionException("old size not zero, but can't victim");
                    }
                }
                auto it_tmp = map_.find(tmp);
                if (it_tmp == map_.end()) {
                    throw ExecutionException("map can't find victim");
                }
                map_.erase(it_tmp);
            } else {
                curr_size_++;
            }
            if (map_[frame_id]->is_old_cache()) {
                old_cache_list_->Unpin(frame_id);
            } else {
                new_cache_list_->Unpin(frame_id);
            }
        }
    } else {
        if (it->second->is_old_cache()) {
            old_cache_list_->Pin(frame_id);
        } else {
            new_cache_list_->Pin(frame_id);
        }
        if (it->second->is_evictable_) {
            curr_size_--;
        }
        map_.erase(it);
    }
    return;
}

// 删除frame的所有相关记录信息, 在BufferPoolManager中删除page的时候调用
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lg(latch_);
    auto it = map_.find(frame_id);
    if (it == map_.end()) {
        return;
    }
    if (!it->second->is_evictable_) {
        return;
    }
    if (it->second->is_old_cache()) {
        old_cache_list_->Pin(frame_id);
    } else {
        new_cache_list_->Pin(frame_id);
    }
    map_.erase(it);
    curr_size_--;
    return;
}

auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> lg(latch_);
    return curr_size_; 
}

}  // namespace bustub
