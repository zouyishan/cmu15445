#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
    if (bpm_ == nullptr || page_ == nullptr) {
        return;
    }
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    return *this; 
}

BasicPageGuard::~BasicPageGuard() {
    if (bpm_ != nullptr && page_ != nullptr) {
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    this->guard_ = std::move(that.guard_);
    return *this; 
}

void ReadPageGuard::Drop() {
    if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
        return;
    }
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
    if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
        guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
        guard_.page_->RUnlatch();
    }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    this->guard_ = std::move(that.guard_);
    return *this; 
}

void WritePageGuard::Drop() {
    if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
        return;
    }
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() {
    if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
        guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
        guard_.page_->WUnlatch();
    }
}  // NOLINT

}  // namespace bustub
