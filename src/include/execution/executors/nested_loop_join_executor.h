//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  void PutLeftJoin(Tuple* tuple) {
    std::vector<Value> val;
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
      val.emplace_back(tuple->GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
      val.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    result_set_.emplace_back(Tuple{val, &GetOutputSchema()});
  }
  void PutInnerJoin(Tuple *left_tuple, Tuple *right_tuple) {
    std::vector<Value> val;
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
      val.emplace_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
      val.emplace_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
    }
    result_set_.emplace_back(Tuple{val, &GetOutputSchema()});
  }
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::vector<Tuple> result_set_;
  std::vector<Tuple>::iterator iterator_;
};

}  // namespace bustub
