//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { 
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  row_count_ = 0;
  has_exec_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  row_count_ = 0;
  auto *transaction = new Transaction(0);
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 从表中删除老数据,这里不确定是新建一个meta还是拿老的meta改
    auto tuple_meta = table_info_->table_->GetTupleMeta(child_rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, child_rid);

    // 从索引中删除老数据
    for (auto &index : index_) {
      auto new_key = child_tuple.KeyFromTuple(table_info_->schema_, *(index->index_->GetKeySchema()),
                                            index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(new_key, child_rid, transaction);
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
