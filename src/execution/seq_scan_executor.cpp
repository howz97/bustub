//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      itr_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  itr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *tbl_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto *pred = plan_->GetPredicate();
  for (; itr_ != tbl_info->table_->End(); ++itr_) {
    *tuple = *itr_;
    *rid = itr_->GetRid();
    if (pred == nullptr || pred->Evaluate(tuple, &tbl_info->schema_).GetAs<bool>()) {
      auto *out_schema = GetOutputSchema();
      std::vector<Value> out_vals;
      for (const auto &col : out_schema->GetColumns()) {
        out_vals.push_back(col.GetExpr()->Evaluate(tuple, &tbl_info->schema_));
      }
      *tuple = Tuple(out_vals, out_schema);
      ++itr_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
