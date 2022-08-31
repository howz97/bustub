//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_->Init();
  }
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  TableInfo *tbl_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  Tuple tup;
  if (plan_->IsRawInsert()) {
    if (raw_val_idx_ >= plan_->RawValues().size()) {
      return false;
    }
    tup = Tuple(plan_->RawValuesAt(raw_val_idx_++), &tbl_info->schema_);
  } else {
    RID discard;
    if (!child_->Next(&tup, &discard)) {
      return false;
    }
  }
  RID rid_ins;
  if (!tbl_info->table_->InsertTuple(tup, &rid_ins, exec_ctx_->GetTransaction())) {
    return false;
  }
  Transaction *txn = exec_ctx_->GetTransaction();
  if (!exec_ctx_->GetLockManager()->LockExclusive(txn, rid_ins)) {
    return false;
  }
  for (IndexInfo *index : exec_ctx_->GetCatalog()->GetTableIndexes(tbl_info->name_)) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = tup.KeyFromTuple(tbl_info->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    txn->GetIndexWriteSet()->emplace_back(rid_ins, tbl_info->oid_, WType::INSERT, tup, index->index_oid_,
                                          exec_ctx_->GetCatalog());
    index->index_->InsertEntry(key, rid_ins, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
