//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/hash_join_plan.h"
namespace bustub {
using ummap = std::unordered_multimap<AggregateKey, bustub::Tuple>;

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto ConcatTuples(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
    std::vector<Value> values;
    for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
      values.emplace_back(left_tuple->GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
    }
    for (size_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
      values.emplace_back(right_tuple->GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
    }
    return {values, &GetOutputSchema()};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  ummap map_;
  std::pair<ummap::iterator, ummap::iterator> range_;
  Tuple left_tuple_;
};
}  // namespace bustub
