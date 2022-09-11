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
  LOG_DEBUG("transaction %d LockShared %s", txn->GetTransactionId(), rid.ToString().c_str());
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // READ_UNCOMMITTED read data without lock
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsSharedLocked(rid)) {
    UNREACHABLE("duplicated lock");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end();) {
    if (it->txn_id_ > txn->GetTransactionId() && it->txn_->GetState() == TransactionState::GROWING) {
      if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        LOG_DEBUG("transaction %d wound %d because conflict on %s", txn->GetTransactionId(), it->txn_id_,
                  rid.ToString().c_str());
        it->txn_->SetState(TransactionState::ABORTED);
        auto blk = blocking_.find(it->txn_id_);
        if (blk != blocking_.end()) {
          lock_table_[blk->second].cv_.notify_all();
        }
        if (!it->granted_) {
          it = queue->request_queue_.erase(it);
          continue;
        }
      } else if (it->txn_id_ == queue->upgrading_) {
        LOG_DEBUG("transaction %d wound %d because conflict on %s", txn->GetTransactionId(), it->txn_id_,
                  rid.ToString().c_str());
        it->txn_->SetState(TransactionState::ABORTED);
        queue->upgrading_ = INVALID_TXN_ID;
        auto blk = blocking_.find(it->txn_id_);
        if (blk != blocking_.end()) {
          lock_table_[blk->second].cv_.notify_all();
        }
      }
    }
    ++it;
  }

  LockRequest &req = queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  req.txn_ = txn;
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto &r : queue->request_queue_) {
      if (r.txn_id_ == txn->GetTransactionId()) {
        return true;
      }
      if (r.lock_mode_ == LockMode::EXCLUSIVE) {
        blocking_[txn->GetTransactionId()] = rid;
        return false;
      }
    }
    UNREACHABLE("request_queue_ must contains this request");
  });
  blocking_.erase(txn->GetTransactionId());
  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == txn->GetTransactionId(); });
    queue->cv_.notify_all();
    return false;
  }
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  LOG_DEBUG("transaction %d LockExclusive %s", txn->GetTransactionId(), rid.ToString().c_str());
  if (txn->IsExclusiveLocked(rid)) {
    UNREACHABLE("duplicated lock");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end();) {
    if (it->txn_id_ > txn->GetTransactionId() && it->txn_->GetState() == TransactionState::GROWING) {
      LOG_DEBUG("transaction %d wound %d because conflict on %s", txn->GetTransactionId(), it->txn_id_,
                rid.ToString().c_str());
      it->txn_->SetState(TransactionState::ABORTED);
      auto blk = blocking_.find(it->txn_id_);
      if (blk != blocking_.end()) {
        lock_table_[blk->second].cv_.notify_all();
      }
      if (!it->granted_) {
        it = queue->request_queue_.erase(it);
        continue;
      }
      if (it->txn_id_ == queue->upgrading_) {
        queue->upgrading_ = INVALID_TXN_ID;
      }
    }
    ++it;
  }

  LockRequest &req = queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  req.txn_ = txn;
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto begin = queue->request_queue_.begin();
    assert(begin != queue->request_queue_.end());
    if (begin->txn_id_ == txn->GetTransactionId()) {
      return true;
    }
    blocking_[txn->GetTransactionId()] = rid;
    return false;
  });
  blocking_.erase(txn->GetTransactionId());
  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == txn->GetTransactionId(); });
    queue->cv_.notify_all();
    return false;
  }
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  LOG_DEBUG("transaction %d LockUpgrade %s", txn_id, rid.ToString().c_str());
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    UNREACHABLE("duplicated lock");
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  if (queue->upgrading_ != INVALID_TXN_ID) {
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }
  // wound younger transactions
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end() && it->granted_;) {
    if (it->txn_id_ > txn_id && it->txn_->GetState() == TransactionState::GROWING) {
      LOG_DEBUG("transaction %d wound %d because conflict on %s", txn_id, it->txn_id_, rid.ToString().c_str());
      it->txn_->SetState(TransactionState::ABORTED);
      auto blk = blocking_.find(it->txn_id_);
      if (blk != blocking_.end()) {
        lock_table_[blk->second].cv_.notify_all();
      }
    }
    ++it;
  }
  queue->upgrading_ = txn_id;
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto &r : queue->request_queue_) {
      if (!r.granted_) {
        break;
      }
      if (r.txn_id_ != txn_id) {
        blocking_[txn_id] = rid;
        return false;
      }
    }
    return true;
  });
  blocking_.erase(txn_id);
  if (txn->GetState() == TransactionState::ABORTED) {
    if (queue->upgrading_ == txn_id) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    return false;
  }
  queue->upgrading_ = INVALID_TXN_ID;
  auto it = queue->request_queue_.begin();
  assert(it != queue->request_queue_.end());
  if (it->txn_id_ != txn_id) {
    LOG_DEBUG("it->txn_id_(%d) != txn_id(%d)", it->txn_id_, txn_id);
    for (auto &r : queue->request_queue_) {
      if (!r.granted_) {
        break;
      }
      std::cout << " txn" << it->txn_id_ << "-" << (it->lock_mode_ == LockMode::EXCLUSIVE);
    }
    std::cout << std::endl;
  }
  assert(it->txn_id_ == txn_id);
  assert(it->lock_mode_ == LockMode::SHARED);
  it->lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  LOG_DEBUG("transaction %d Unlock %s", txn->GetTransactionId(), rid.ToString().c_str());
  std::lock_guard lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  auto it = queue->request_queue_.begin();
  for (; it != queue->request_queue_.end(); ++it) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  assert(it != queue->request_queue_.end());
  if (txn->GetState() == TransactionState::GROWING) {
    // READ_COMMITTED may release shared-lock at growing state
    if (it->lock_mode_ == LockMode::EXCLUSIVE || txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  queue->request_queue_.erase(it);
  if (queue->upgrading_ != INVALID_TXN_ID || !queue->IsLocked()) {
    queue->cv_.notify_all();
    // LOG_DEBUG("transaction %d notify_all", txn->GetTransactionId());
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

auto LockManager::LockRequestQueue::IsLocked() -> bool {
  auto begin = request_queue_.begin();
  return begin != request_queue_.end() && begin->granted_;
}

}  // namespace bustub
