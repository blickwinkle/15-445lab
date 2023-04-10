//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_);
  row_count_ = 0;
  has_exec_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  auto *transaction = new Transaction(0);
  row_count_ = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto inserted_rid = table_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, child_tuple);
    if (inserted_rid == std::nullopt) {
      delete transaction;
      return false;
    }

    for (auto &index : index_) {
      auto new_key =
          child_tuple.KeyFromTuple(table_->schema_, *(index->index_->GetKeySchema()), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key, *inserted_rid, transaction);
    }
    row_count_++;
  }
  delete transaction;
  if (has_exec_) {
    return false;
  }
  has_exec_ = true;
  *tuple = Tuple({ValueFactory::GetIntegerValue(row_count_)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
