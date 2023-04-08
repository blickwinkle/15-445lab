//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_->table_name_);

    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_->index_.get());
    iterator_ = tree->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (!iterator_.IsEnd() && table_info_->table_->GetTuple((*iterator_).second).first.is_deleted_) {
        ++iterator_;
    }
    if (iterator_.IsEnd()) {
        return false;
    }
    *rid = (*iterator_).second;
    *tuple = table_info_->table_->GetTuple(*rid).second;
    ++iterator_;
    return true;
}

}  // namespace bustub
