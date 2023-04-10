//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();

  auto filter_expr = plan_->Predicate();
  Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;
  result_set_.clear();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    bool has_emplace = false;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = filter_expr->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                             right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        PutInnerJoin(&left_tuple, &right_tuple);
        has_emplace = true;
      }
    }
    if (!has_emplace && plan_->GetJoinType() == JoinType::LEFT) {
      PutLeftJoin(&left_tuple);
    }
  }
  iterator_ = result_set_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  ++iterator_;
  return true;
}

}  // namespace bustub
