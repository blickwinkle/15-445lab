#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto IsCompEqulExpr(const AbstractExpressionRef &expr) -> bool {
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); comp_expr != nullptr) {
    return comp_expr->comp_type_ == ComparisonType::Equal;
  }
  return false;
}

auto IsAndLogicExpr(const AbstractExpressionRef &expr) -> bool {
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
    return logic_expr->logic_type_ == LogicType::And;
  }
  return false;
}

// auto Is;

static auto CopyExprAndLookUpTupleIndex(const AbstractExpressionRef &expr, int &tuple_index) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(CopyExprAndLookUpTupleIndex(child, tuple_index));
  }
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    tuple_index = column_value_expr->GetTupleIdx();
    // auto ret = column_value_expr->CloneWithChildren(children);
    // auto tmp_p = dynamic_cast<ColumnValueExpression *>(ret.get());
    return std::make_shared<ColumnValueExpression>(0, column_value_expr->GetColIdx(), column_value_expr->GetReturnType());
    
  }
  return expr->CloneWithChildren(children);
}

static auto Solve(std::vector<AbstractExpressionRef> *expressions, const ComparisonExpression *comp_expr) {
  int tuple_index = -1;
  auto copy_expr = CopyExprAndLookUpTupleIndex(comp_expr->GetChildAt(0), tuple_index);
  expressions[tuple_index].emplace_back(std::move(copy_expr));
  
  tuple_index = -1;
  copy_expr = CopyExprAndLookUpTupleIndex(comp_expr->GetChildAt(1), tuple_index);
  expressions[tuple_index].emplace_back(std::move(copy_expr));
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (IsCompEqulExpr(nlj_plan.Predicate())) {
      const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());

      std::vector<AbstractExpressionRef> expressions[2];
      Solve(expressions, comp_expr);
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), std::move(expressions[0]), std::move(expressions[1]), nlj_plan.join_type_);
    } 
    if (IsAndLogicExpr(nlj_plan.Predicate())) {
      const auto *logic_expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
      if (IsCompEqulExpr(logic_expr->GetChildAt(0)) && IsCompEqulExpr(logic_expr->GetChildAt(1))) {
        std::vector<AbstractExpressionRef> expressions[2];
        const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(0).get());
        Solve(expressions, comp_expr);
        comp_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(1).get());
        Solve(expressions, comp_expr);
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), std::move(expressions[0]), std::move(expressions[1]), nlj_plan.join_type_);
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
