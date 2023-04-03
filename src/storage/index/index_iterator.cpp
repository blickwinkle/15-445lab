/**
 * index_iterator.cpp
 */
#include <cassert>

#include "argparse/argparse.hpp"
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t pid, BufferPoolManager *bpm_, int ind) : bpm_(bpm_), pid_(pid), ind_(ind) {
    leaf_pg_guard_ = bpm_->FetchPageRead(pid);
    pg_pointer_ = leaf_pg_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    //ind_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    leaf_pg_guard_.Drop();
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
    // return pg_pointer_->GetNextPageId() == INVALID_PAGE_ID && ind_ == pg_pointer_->GetSize();
    return ind_ == pg_pointer_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    if (IsEnd()) { 
        BUSTUB_ASSERT(false, "*END\n");
        pg_pointer_->ArrayAt(ind_ - 1); 
    }
    return pg_pointer_->ArrayAt(ind_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    if (IsEnd()) { 
        BUSTUB_ASSERT(false, "++END\n");
        return *this; 
    }
    ind_++;
    if (ind_ < pg_pointer_->GetSize()) {
        return *this;
    }
    if (pg_pointer_->GetNextPageId() == INVALID_PAGE_ID) {return *this; }
    ind_ = 0;

    page_id_t newid = pg_pointer_->GetNextPageId();
    leaf_pg_guard_.Drop();

    leaf_pg_guard_ = bpm_->FetchPageRead(newid);
    pg_pointer_ = leaf_pg_guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
