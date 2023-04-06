/**
 * index_iterator.cpp
 */
#include <cassert>

#include "argparse/argparse.hpp"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t pid, BufferPoolManager *bpm, int ind) : bpm_(bpm), pid_(pid), ind_(ind) {
  // leaf_pg_guard_ = bpm_->FetchPageRead(pid);
  // pg_pointer_ = leaf_pg_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  // ind_ = 0;
}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool {
  // return pg_pointer_->GetNextPageId() == INVALID_PAGE_ID && ind_ == pg_pointer_->GetSize();
  // return ind_ == pg_pointer_->GetSize();
  ReadPageGuard pg_guard = bpm_->FetchPageRead(pid_);
  const auto *pg_pointer = pg_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return IsEndP(pg_pointer);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  ReadPageGuard pg_guard = bpm_->FetchPageRead(pid_);
  const auto *pg_pointer = pg_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (IsEndP(pg_pointer)) {
    LOG_ERROR("*END ITERATOR pid : %d, ind_ : %d", pid_, ind_);
    BUSTUB_ASSERT(false, "*END\n");
  }
  return pg_pointer->ArrayAt(ind_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ReadPageGuard pg_guard = bpm_->FetchPageRead(pid_);
  const auto *pg_pointer = pg_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (IsEndP(pg_pointer)) {
    BUSTUB_ASSERT(false, "++END\n");
  }
  ind_++;
  if (ind_ < pg_pointer->GetSize() || pg_pointer->GetNextPageId() == INVALID_PAGE_ID) {
    return *this;
  }

  ind_ = 0;
  pid_ = pg_pointer->GetNextPageId();
  // pg_guard = bpm_->FetchPageRead(newid);
  // pg_pointer_ = pg_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
