#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;

  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (this->bpm_ != nullptr) {
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
  }
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;

  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.bpm_ == nullptr) {
    return;
  }
  this->guard_.page_->RUnlatch();
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.bpm_ == nullptr) {
    return;
  }
  this->guard_.page_->WUnlatch();
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
