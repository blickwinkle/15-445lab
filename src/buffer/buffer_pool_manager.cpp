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
#include <tuple>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
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

auto BufferPoolManager::AllocateFrame(page_id_t page_id, frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.erase(free_list_.begin());
  } else {
    if (replacer_->Evict(frame_id)) {
      if (pages_[*frame_id].IsDirty()) {
        FlushPageNoLock(pages_[*frame_id].GetPageId());
      }
      page_table_.erase(pages_[*frame_id].GetPageId());
    } else {
      return false;
    }
  }
  pages_[*frame_id].ResetMemory();
  pages_[*frame_id].page_id_ = page_id;
  pages_[*frame_id].is_dirty_ = false;
  pages_[*frame_id].pin_count_ = 1;
  page_table_[page_id] = *frame_id;
  replacer_->SetEvictable(*frame_id, false);
  replacer_->RecordAccess(*frame_id);
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;
  page_id_t new_page_id = AllocatePage();
  latch_.lock();
  if (!AllocateFrame(new_page_id, &frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  latch_.unlock();
  *page_id = new_page_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id = -1;
  latch_.lock();
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
  }

  if (!AllocateFrame(page_id, &frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  latch_.unlock();
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ = is_dirty || pages_[frame_id].is_dirty_;

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPageNoLock(page_id_t page_id) -> bool {
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  if (FlushPageNoLock(page_id)) {
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto &it : page_table_) {
    FlushPageNoLock(it.first);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  pages_[frame_id].page_id_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
  free_list_.push_front(frame_id);

  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // return {this, nullptr};
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // return {this, nullptr};
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // return {this, nullptr};
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // return {this, nullptr};
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
