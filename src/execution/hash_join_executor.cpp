//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      range_(std::pair(map_.end(), map_.end())) {}

/** Initialize the join */
void HashJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_child_->Init();
  // if (!left_child_->Next(&tuple, &rid)) {
  //   // left child is empty, no need to initialize hashtable
  //   return;
  // }
  // left_child_->Init();

  right_child_->Init();
  auto schema = right_child_->GetOutputSchema();
  while (right_child_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    for (auto exp : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(exp->Evaluate(&tuple, schema));
    }
    AggregateKey hjk(std::move(values));
    map_.insert(std::pair(hjk, tuple));
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_child_->GetOutputSchema();
  while (range_.first == range_.second) {
    RID discard;
    if (!left_child_->Next(&left_tuple_, &discard)) {
      return false;
    }
    std::vector<Value> values;
    for (auto exp : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(exp->Evaluate(&left_tuple_, left_schema));
    }
    AggregateKey hjk(std::move(values));
    range_ = map_.equal_range(hjk);
  }
  Tuple right_tuple = (range_.first++)->second;
  *tuple = ConcatTuples(&left_tuple_, &right_tuple);
  return true;
}

}  // namespace bustub
