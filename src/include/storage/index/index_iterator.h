//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t pid, BufferPoolManager *bpm_, int ind);
  IndexIterator() : bpm_(nullptr), pid_(INVALID_PAGE_ID) {}
  ~IndexIterator() = default;  // NOLINT

  auto IsEnd() const -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (this->pid_ == itr.pid_ && this->bpm_ == itr.bpm_ && this->ind_ == itr.ind_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  // ReadPageGuard leaf_pg_guard_;
  inline auto IsEmpty() const -> bool { return bpm_ == nullptr || pid_ == INVALID_PAGE_ID; }
  inline auto IsEndP(const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *pg_pointer_) const -> bool {
    return pg_pointer_->GetNextPageId() == INVALID_PAGE_ID && ind_ >= pg_pointer_->GetSize();
  }
  BufferPoolManager *bpm_;
  page_id_t pid_;
  // const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *pg_pointer_;
  int ind_;
};

}  // namespace bustub
