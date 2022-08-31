//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple tp;
  RID r;
  if (!child_executor_->Next(&tp, &r)) {
    return false;
  }
  Transaction *txn = exec_ctx_->GetTransaction();
  // acquire lock
  bool locked = false;
  if (txn->IsSharedLocked(r)) {
    locked = exec_ctx_->GetLockManager()->LockUpgrade(txn, r);
  } else if (txn->IsExclusiveLocked(r)) {
    locked = true;
  } else {
    locked = exec_ctx_->GetLockManager()->LockExclusive(txn, r);
  }
  if (!locked) {
    return false;
  }

  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (!table_info->table_->MarkDelete(r, txn)) {
    return false;
  }
  for (IndexInfo *index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = tp.KeyFromTuple(table_info->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    txn->GetIndexWriteSet()->emplace_back(r, table_info->oid_, WType::DELETE, tp, index->index_oid_,
                                          exec_ctx_->GetCatalog());
    index->index_->DeleteEntry(key, r, txn);
  }
  return true;
}

}  // namespace bustub
