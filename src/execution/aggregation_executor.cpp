//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(nullptr) {}

void AggregationExecutor::Init() {
  Tuple child_tuple;
  RID child_rid;
  child_->Init();
  aht_.Clear();
  while (child_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = std::make_shared<SimpleAggregationHashTable::Iterator>(aht_.Begin());
  is_first_next_ = true;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*aht_iterator_ == aht_.End()) {
    if (is_first_next_) {
      is_first_next_ = false;
      if (!plan_->GetGroupBys().empty()) {
        return false;
      }
      *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
      return true;
    }
    return false;
  }
  is_first_next_ = false;
  std::vector<Value> merged_value;
  merged_value.insert(merged_value.end(), aht_iterator_->Key().group_bys_.begin(),
                      aht_iterator_->Key().group_bys_.end());
  merged_value.insert(merged_value.end(), aht_iterator_->Val().aggregates_.begin(),
                      aht_iterator_->Val().aggregates_.end());

  *tuple = {std::move(merged_value), &GetOutputSchema()};
  ++*aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
