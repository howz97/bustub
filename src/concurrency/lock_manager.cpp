//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  LockRequest &req = queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto r : queue->request_queue_) {
      if (r.txn_id_ == txn->GetTransactionId()) {
        return true;
      }
      if (r.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    UNREACHABLE("request_queue_ must contains this request");
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.remove(req);
    queue->cv_.notify_all();
    return false;
  }
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  LockRequest &req = queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto begin = queue->request_queue_.begin();
    assert(begin != queue->request_queue_.end());
    return begin->txn_id_ == txn->GetTransactionId();
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.remove(req);
    queue->cv_.notify_all();
    return false;
  }
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  if (queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }
  queue->upgrading_ = txn->GetTransactionId();
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto r : queue->request_queue_) {
      if (!r.granted_) {
        break;
      }
      if (r.txn_id_ != txn->GetTransactionId()) {
        return false;
      }
    }
    return true;
  });
  queue->upgrading_ = INVALID_TXN_ID;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  auto it = queue->request_queue_.begin();
  assert(it != queue->request_queue_.end());
  assert(it->txn_id_ == txn->GetTransactionId());
  assert(it->lock_mode_ == LockMode::SHARED);
  it->lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::lock_guard lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  txn_id_t txn_id = txn->GetTransactionId();
  queue->request_queue_.remove_if([&](LockRequest req) { return req.txn_id_ == txn_id; });
  if (queue->upgrading_ != INVALID_TXN_ID || !queue->IsLocked()) {
    queue->cv_.notify_all();
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

auto LockManager::LockRequestQueue::IsLocked() -> bool {
  auto begin = request_queue_.begin();
  return begin != request_queue_.end() && !begin->granted_;
}

}  // namespace bustub
