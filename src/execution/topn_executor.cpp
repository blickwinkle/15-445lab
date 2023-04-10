#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  auto cmp = [&](const Tuple &a, const Tuple &b) -> bool {
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
  };
  result_set_ = {};
  child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> queue(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    queue.push(tuple);
    if (queue.size() > plan_->GetN()) {
      queue.pop();
    }
  }
  while (!queue.empty()) {
    result_set_.push(queue.top());
    queue.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_set_.empty()) {
    return false;
  }
  *tuple = result_set_.top();
  result_set_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return result_set_.size(); };

}  // namespace bustub
