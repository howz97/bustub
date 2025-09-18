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

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID discard;
  while (child_executor_->Next(&tuple, &discard)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
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

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
