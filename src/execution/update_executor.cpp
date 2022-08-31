//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
  std::unordered_map<uint32_t, UpdateInfo> update_attrs = plan_->GetUpdateAttr();
  for (IndexInfo *index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    for (auto attr : index->index_->GetKeyAttrs()) {
      if (update_attrs.find(attr) != update_attrs.end()) {
        indexes_.push_back(index);
        break;
      }
    }
  }
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple old_tp;
  RID r;
  if (!child_executor_->Next(&old_tp, &r)) {
    return false;
  }
  Tuple new_tp = GenerateUpdatedTuple(old_tp);
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

  if (!table_info_->table_->UpdateTuple(new_tp, r, exec_ctx_->GetTransaction())) {
    return false;
  }
  // update related indexes
  for (IndexInfo *index : indexes_) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = old_tp.KeyFromTuple(table_info_->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    IndexWriteRecord rec =
        IndexWriteRecord(r, table_info_->oid_, WType::UPDATE, new_tp, index->index_oid_, exec_ctx_->GetCatalog());
    rec.old_tuple_ = old_tp;
    txn->GetIndexWriteSet()->push_back(std::move(rec));
    index->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
    key = new_tp.KeyFromTuple(table_info_->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    index->index_->InsertEntry(key, r, exec_ctx_->GetTransaction());
  }
  return true;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
