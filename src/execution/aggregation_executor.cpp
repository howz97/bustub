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
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID discard;
  while (child_->Next(&tuple, &discard)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto having = plan_->GetHaving();
  for (; aht_iterator_ != aht_.End(); ++aht_iterator_) {
    std::vector<Value> gby = aht_iterator_.Key().group_bys_;
    std::vector<Value> agg = aht_iterator_.Val().aggregates_;
    if (having != nullptr && !having->EvaluateAggregate(gby, agg).GetAs<bool>()) {
      continue;
    }
    std::vector<Value> out_vals;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      out_vals.push_back(col.GetExpr()->EvaluateAggregate(gby, agg));
    }
    *tuple = Tuple(out_vals, plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
