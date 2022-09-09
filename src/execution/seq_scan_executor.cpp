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
      itr_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {
  itr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *tbl_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto *pred = plan_->GetPredicate();
  for (; itr_ != tbl_info->table_->End(); ++itr_) {
    *tuple = *itr_;
    *rid = itr_->GetRid();
    Transaction *txn = exec_ctx_->GetTransaction();
    // acquire shared lock
    bool locked = false;
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (!txn->IsSharedLocked(*rid)) {
        locked = exec_ctx_->GetLockManager()->LockShared(txn, *rid);
      }
    } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      locked = exec_ctx_->GetLockManager()->LockShared(txn, *rid);
    }
    if (!locked) {
      return false;
    }

    if (pred == nullptr || pred->Evaluate(tuple, &tbl_info->schema_).GetAs<bool>()) {
      auto *out_schema = GetOutputSchema();
      std::vector<Value> out_vals;
      for (const auto &col : out_schema->GetColumns()) {
        out_vals.push_back(col.GetExpr()->Evaluate(tuple, &tbl_info->schema_));
      }
      *tuple = Tuple(out_vals, out_schema);
      ++itr_;
      // release lock
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->Unlock(txn, *rid);
      }
      return true;
    }
    // release lock
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(txn, *rid);
    }
  }
  return false;
}

}  // namespace bustub
