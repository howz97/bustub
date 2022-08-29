//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

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

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple old_tp;
  RID r;
  if (!child_executor_->Next(&old_tp, &r)) {
    return false;
  }
  Tuple new_tp = GenerateUpdatedTuple(old_tp);
  if (!table_info_->table_->UpdateTuple(new_tp, r, exec_ctx_->GetTransaction())) {
    return false;
  }
  // update related indexes
  for (IndexInfo *index : indexes_) {
    IndexMetadata *meta = index->index_->GetMetadata();
    Tuple key = old_tp.KeyFromTuple(table_info_->schema_, *meta->GetKeySchema(), meta->GetKeyAttrs());
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
