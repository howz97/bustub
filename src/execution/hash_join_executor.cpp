//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      range_(std::pair(map_.end(), map_.end())) {}

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
    auto v = plan_->RightJoinKeyExpression()->Evaluate(&tuple, schema);
    AggregateKey hjk = MakeHJKey(v);
    map_.insert(std::pair(hjk, tuple));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_child_->GetOutputSchema();
  while (range_.first == range_.second) {
    RID discard;
    if (!left_child_->Next(&left_tuple_, &discard)) {
      return false;
    }
    auto k = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple_, left_schema);
    range_ = map_.equal_range(MakeHJKey(k));
  }
  auto right_schema = right_child_->GetOutputSchema();
  auto out_schema = plan_->OutputSchema();
  Tuple right_tuple = (range_.first++)->second;
  std::vector<Value> vals;
  for (const auto &col : out_schema->GetColumns()) {
    vals.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema));
  }
  *tuple = Tuple(vals, out_schema);
  return true;
}

}  // namespace bustub
