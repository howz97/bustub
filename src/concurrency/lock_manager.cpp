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
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // READ_UNCOMMITTED read data without lock
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end(); ++it) {
    if (it->txn_id_ > txn->GetTransactionId()) {
      if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        it->txn_->SetState(TransactionState::ABORTED);
        if (!it->granted_) {
          // TODO(zhanghao): maybe notify_all ?
          queue->request_queue_.erase(it);
        }
      } else if (it->txn_id_ == queue->upgrading_) {
        it->txn_->SetState(TransactionState::ABORTED);
        queue->upgrading_ = INVALID_TXN_ID;
      }
    }
  }

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
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == req.txn_id_; });
    queue->cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end(); ++it) {
    if (it->txn_id_ > txn->GetTransactionId()) {
      it->txn_->SetState(TransactionState::ABORTED);
      if (!it->granted_) {
        queue->request_queue_.erase(it);
      }
      if (it->txn_id_ == queue->upgrading_) {
        queue->upgrading_ = INVALID_TXN_ID;
      }
    }
  }

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
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == req.txn_id_; });
    queue->cv_.notify_all();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  if (queue->upgrading_ != INVALID_TXN_ID) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
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
  if (txn->GetState() == TransactionState::ABORTED) {
    if (queue->upgrading_ == txn->GetTransactionId()) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  queue->upgrading_ = INVALID_TXN_ID;
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
  auto it = queue->request_queue_.begin();
  for (; it != queue->request_queue_.end(); ++it) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    // READ_COMMITTED may release shared-lock at growing state
    if (it->lock_mode_ == LockMode::EXCLUSIVE || txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  queue->request_queue_.erase(it);
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
