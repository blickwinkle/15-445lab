//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
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
  void PutInnerJoin(const Tuple *left_tuple, const Tuple *right_tuple) {
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
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::vector<Tuple> result_set_;
  std::vector<Tuple>::iterator iterator_;
  std::unordered_map<AggregateKey, std::vector<Tuple>> right_table_;
};

}  // namespace bustub
