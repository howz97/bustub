//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  TableInfo *tbl_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (plan_->IsRawInsert()) {
    if (raw_val_idx_ >= plan_->RawValues().size()) {
      return false;
    }
    *tuple = Tuple(plan_->RawValuesAt(raw_val_idx_++), &tbl_info->schema_);
  } else {
    if (!child_->Next(tuple, rid)) {
      return false;
    }
  }
  if (!tbl_info->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (IndexInfo *index : exec_ctx_->GetCatalog()->GetTableIndexes(tbl_info->name_)) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = tuple->KeyFromTuple(tbl_info->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
