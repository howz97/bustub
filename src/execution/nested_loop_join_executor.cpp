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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  RID discard;
  if (left_executor_->Next(&left_tuple_, &discard)) {
    right_executor_->Init();
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      RID discard;
      if (!left_executor_->Next(&left_tuple_, &discard)) {
        return false;
      }
      right_executor_->Init();
      if (!right_executor_->Next(&right_tuple, &right_rid)) {
        return false;
      }
    }
    auto pred = plan_->Predicate();
    if (pred == nullptr) {
      break;
    }
    auto v = pred->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                right_executor_->GetOutputSchema());
    if (v.GetAs<bool>()) {
      break;
    }
  }
  std::vector<Value> vals;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    vals.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(vals, GetOutputSchema());
  return true;
}

}  // namespace bustub
