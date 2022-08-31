//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {
struct DistnKey {
  std::vector<Value> list_;

  auto operator==(const DistnKey &other) const -> bool {
    for (uint32_t i = 0; i < other.list_.size(); i++) {
      if (list_[i].CompareEquals(other.list_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the distinct */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  auto MakeDistinctKey(Tuple *tuple) -> AggregateKey {
    auto schema = child_executor_->GetOutputSchema();
    std::vector<Value> vals;
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      vals.push_back(tuple->GetValue(schema, i));
    }
    return AggregateKey{vals};
  }
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::unordered_map<AggregateKey, bool> dedup_;
};
}  // namespace bustub
