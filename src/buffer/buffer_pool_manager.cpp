//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  std::lock_guard<std::mutex> lg(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    memset(pages_[frame_id].data_, 0, strlen(pages_[frame_id].data_));
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  if (replacer_->Size() != 0) {
    *page_id = AllocatePage();
    bool flag = replacer_->Evict(&frame_id);
    if (flag) {
      assert(pages_[frame_id].pin_count_ == 0);
      if (pages_[frame_id].IsDirty()) {
        // std::cout << "dirty write page: " << pages_[frame_id].GetPageId() << std::endl;
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      auto it = page_table_.find(pages_[frame_id].GetPageId());
      assert(it != page_table_.end());
      page_table_.erase(it);
      page_table_[*page_id] = frame_id;

      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].page_id_ = *page_id;
      pages_[frame_id].pin_count_ = 1;
      // disk_manager_->ReadPage(*page_id, pages_[frame_id].GetData());
      memset(pages_[frame_id].data_, 0, strlen(pages_[frame_id].data_));

      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    } else {
      assert(false);
    }
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lg(latch_);
  auto it = page_table_.find(page_id);
  frame_id_t frame_id;
  if (it != page_table_.end()) {
    frame_id = it->second;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = frame_id;
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }

  if (replacer_->Size() != 0) {
    bool flag = replacer_->Evict(&frame_id);
    if (flag) {
      // std::cout << frame_id << std::endl;
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      auto it = page_table_.find(pages_[frame_id].GetPageId());
      assert(it != page_table_.end());
      page_table_.erase(it);
      page_table_[page_id] = frame_id;

      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].pin_count_ = 1;
      memset(pages_[frame_id].data_, 0, strlen(pages_[frame_id].data_));

      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    }
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lg(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id;
  frame_id = it->second;
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ = is_dirty;
  if (pages_[frame_id].pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> lg(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id;
  frame_id = it->second;
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lg(latch_);
  auto it = page_table_.begin();
  for (; it != page_table_.end(); it++) {
    FlushPage(it->first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> lg(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id;
  frame_id = it->second;
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  memset(pages_[frame_id].data_, 0, strlen(pages_[frame_id].data_));

  page_table_.erase(it);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { 
  // std::lock_guard<std::mutex> lg(latch_);
  return next_page_id_++; 
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::mutex> lg(latch_);
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  // std::lock_guard<std::mutex> lg(latch_);
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  // std::lock_guard<std::mutex> lg(latch_);
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page}; 
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // std::lock_guard<std::mutex> lg(latch_);
  return {this, NewPage(page_id)}; 
}

}  // namespace bustub
