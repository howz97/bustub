//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple tp;
  RID r;
  if (!child_executor_->Next(&tp, &r)) {
    return false;
  }
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (!table_info->table_->MarkDelete(r, exec_ctx_->GetTransaction())) {
    return false;
  }
  for (IndexInfo *index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = tp.KeyFromTuple(table_info->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
    index->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
  }
  return true;
}

}  // namespace bustub
