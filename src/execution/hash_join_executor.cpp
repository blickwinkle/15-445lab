//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) , plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() { 
  result_set_.clear();
  right_table_.clear();

  left_executor_->Init();
  right_executor_->Init();
  auto right_expr = plan_->RightJoinKeyExpressions();
  auto left_expr = plan_->LeftJoinKeyExpressions();
  Tuple tuple;
  RID rid;
  
  while (right_executor_->Next(&tuple, &rid)) {
    std::vector<Value> key_set;
    key_set.reserve(right_expr.size());
    for (auto &expr : right_expr) {
      key_set.emplace_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
    }
    right_table_[{key_set}].emplace_back(tuple);
  }
  while (left_executor_->Next(&tuple, &rid)) {
    std::vector<Value> key_set;
    key_set.reserve(left_expr.size());
    for (auto &expr : left_expr) {
      key_set.emplace_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
    }
    if (right_table_.count({key_set}) == 0) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        PutLeftJoin(&tuple);
      }
    } else {
      for (const auto &right_tuple : right_table_.at({key_set})) {
        PutInnerJoin(&tuple, &right_tuple);
      }
    }
  }
  iterator_ = result_set_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  ++iterator_;
  return true;
}

}  // namespace bustub
