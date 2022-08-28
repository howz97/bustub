//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      itr_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *tbl_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  for (; itr_ != tbl_info->table_->End(); ++itr_) {
    *tuple = *itr_;
    *rid = itr_->GetRid();
    if (plan_->GetPredicate()->Evaluate(tuple, &tbl_info->schema_).GetAs<bool>()) {
      ++itr_;
      return true;
    };
  }
  return false;
}

}  // namespace bustub
