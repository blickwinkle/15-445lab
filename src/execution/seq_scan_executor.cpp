//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  iter_ =
      std::make_shared<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd() && iter_->GetTuple().first.is_deleted_) {
    ++*iter_;
  }
  if (iter_->IsEnd()) {
    return false;
  }
  *tuple = iter_->GetTuple().second;
  *rid = iter_->GetRID();
  ++*iter_;
  return true;
}

}  // namespace bustub
