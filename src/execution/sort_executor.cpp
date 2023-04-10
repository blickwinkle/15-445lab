#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "common/logger.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  result_set_.clear();
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    result_set_.emplace_back(tuple);
  }

  std::sort(result_set_.begin(), result_set_.end(), [&](Tuple &a, Tuple &b) -> bool {
    for (auto &order : plan_->GetOrderBy()) {
      auto v1 = order.second->Evaluate(&a, child_executor_->GetOutputSchema());
      auto v2 = order.second->Evaluate(&b, child_executor_->GetOutputSchema());
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }
      if (order.first == OrderByType::DESC) {
        return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
      }
      return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
    }
    // LOG_ERROR("failed compare!");
    return true;
  });
  iterator_ = result_set_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == result_set_.end()) {
    return false;
  }
  *tuple = *iterator_;
  ++iterator_;
  return true;
}

}  // namespace bustub
