//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/config.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  row_count_ = 0;
  has_exec_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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

    // 通过表达式计算出新的values
    std::vector<Value> insert_values;
    for (auto &expr : plan_->target_expressions_) {
      insert_values.emplace_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    // 插入新的tuple
    Tuple insert_tuple{std::move(insert_values), &child_executor_->GetOutputSchema()};
    auto inserted_rid = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, insert_tuple);
    if (inserted_rid == std::nullopt) {
      delete transaction;
      return false;
    }
    // 同步索引
    for (auto &index : index_) {
      auto new_key = insert_tuple.KeyFromTuple(table_info_->schema_, *(index->index_->GetKeySchema()),
                                               index->index_->GetKeyAttrs());
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
