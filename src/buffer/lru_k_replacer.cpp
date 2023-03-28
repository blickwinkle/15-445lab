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
#include <climits>
#include <cstddef>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  frame_id_t evict_id = -1;
  // size_t evict_k = 0;

  latch_.lock();
  for (auto const &it : node_store_) {
    if (!it.second.is_evictable_) {
      continue;
    }
    if (evict_id == -1) {
      evict_id = it.first;
    } else {
      const LRUKNode &old_node = node_store_[evict_id];
      if (old_node.history_.size() >= this->k_) {
        if (it.second.history_.size() < this->k_ || it.second.k_ < old_node.k_) {
          evict_id = it.first;
        }
      } else {
        if (it.second.history_.size() < this->k_ && it.second.k_ < old_node.k_) {
          evict_id = it.first;
        }
      }
    }
  }
  latch_.unlock();
  if (evict_id == -1) {
    return false;
  }
  Remove(evict_id);
  *frame_id = evict_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();

  LRUKNode &node = node_store_[frame_id];
  size_t time = ++current_timestamp_;
  node.fid_ = frame_id;
  node.history_.push_front(time);
  if (node.history_.size() >= this->k_) {
    auto iter = node.history_.begin();
    for (size_t i = 0; i < this->k_ - 1; i++) {
      iter++;
    }
    node.k_ = *iter;
  } else {
    node.k_ = node.history_.back();
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  LRUKNode &node = node_store_[frame_id];
  node.fid_ = frame_id;
  if (set_evictable != node.is_evictable_) {
    if (set_evictable) {
      this->curr_size_++;
    } else {
      this->curr_size_--;
    }
    node.is_evictable_ = set_evictable;
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (node_store_.count(frame_id) != 0) {
    if (node_store_[frame_id].is_evictable_) {
      this->curr_size_--;
    }
    node_store_.erase(frame_id);
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
