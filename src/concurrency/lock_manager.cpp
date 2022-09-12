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

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"

namespace bustub {
auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  // LOG_DEBUG("transaction %d LockShared %s", txn_id, rid.ToString().c_str());
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // READ_UNCOMMITTED read data without lock
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsSharedLocked(rid)) {
    UNREACHABLE("duplicated lock");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  bool xfound = false;
  for (auto it = queue->request_queue_.rbegin(); it != queue->request_queue_.rend(); ++it) {
    if (it->lock_mode_ == LockMode::EXCLUSIVE || it->txn_id_ == queue->upgrading_) {
      xfound = true;
    }
    if (it->txn_id_ > txn_id && it->txn_->GetState() == TransactionState::GROWING && xfound) {
      // LOG_DEBUG("transaction %d wound %d because conflict on %s", txn_id, it->txn_id_, rid.ToString().c_str());
      it->txn_->SetState(TransactionState::ABORTED);
      auto blk = blocking_.find(it->txn_id_);
      if (blk != blocking_.end()) {
        lock_table_[blk->second].cv_.notify_all();
      }
    }
  }

  {
    LockRequest &req = queue->request_queue_.emplace_back(txn_id, LockMode::SHARED);
    req.txn_ = txn;
  }
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    if (queue->upgrading_ != INVALID_TXN_ID) {
      blocking_[txn_id] = rid;
      return false;
    }
    for (auto &r : queue->request_queue_) {
      if (r.txn_id_ == txn_id) {
        return true;
      }
      if (r.lock_mode_ == LockMode::EXCLUSIVE) {
        blocking_[txn_id] = rid;
        return false;
      }
    }
    UNREACHABLE("request_queue_ must contains this request");
  });
  blocking_.erase(txn_id);
  if (txn->GetState() == TransactionState::ABORTED) {
    // LOG_DEBUG("transaction %d start abort", txn_id);
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == txn_id; });
    queue->cv_.notify_all();
    return false;
  }
  queue->Grant(txn_id);
  txn->GetSharedLockSet()->emplace(rid);
  // LOG_DEBUG("transaction %d got shared-lock", txn_id);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  // LOG_DEBUG("transaction %d LockExclusive %s", txn_id, rid.ToString().c_str());
  if (txn->IsExclusiveLocked(rid)) {
    UNREACHABLE("duplicated lock");
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  // wound younger transactions
  for (auto &req : queue->request_queue_) {
    if (req.txn_id_ > txn_id && req.txn_->GetState() == TransactionState::GROWING) {
      // LOG_DEBUG("transaction %d wound %d because conflict on %s", txn_id, it->txn_id_, rid.ToString().c_str());
      req.txn_->SetState(TransactionState::ABORTED);
      auto blk = blocking_.find(req.txn_id_);
      if (blk != blocking_.end()) {
        lock_table_[blk->second].cv_.notify_all();
      }
    }
  }

  {
    LockRequest &req = queue->request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);
    req.txn_ = txn;
  }
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto head = queue->request_queue_.begin();
    if (head->txn_id_ != txn_id) {
      blocking_[txn_id] = rid;
      return false;
    }
    return true;
  });
  blocking_.erase(txn_id);
  if (txn->GetState() == TransactionState::ABORTED) {
    // LOG_DEBUG("transaction %d start abort", txn_id);
    queue->request_queue_.remove_if([&](LockRequest r) { return r.txn_id_ == txn_id; });
    queue->cv_.notify_all();
    return false;
  }
  queue->Grant(txn_id);
  txn->GetExclusiveLockSet()->emplace(rid);
  // LOG_DEBUG("transaction %d got exclusivs-lock", txn_id);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  // LOG_DEBUG("transaction %d LockUpgrade %s", txn_id, rid.ToString().c_str());
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
  for (auto &req : queue->request_queue_) {
    if (req.lock_mode_ != LockMode::SHARED) {
      break;
    }
    if (req.txn_id_ > txn_id && req.txn_->GetState() == TransactionState::GROWING && req.granted_) {
      // LOG_DEBUG("transaction %d wound %d because conflict on %s", txn_id, it->txn_id_, rid.ToString().c_str());
      req.txn_->SetState(TransactionState::ABORTED);
      auto blk = blocking_.find(req.txn_id_);
      if (blk != blocking_.end()) {
        lock_table_[blk->second].cv_.notify_all();
      }
    } else if (req.txn_id_ == txn_id) {
      // consider this situation:
      //   request_queue_: (txn4,ungranted) (txn2,granted) (txn1,granted) (txn3,granted)
      // txn2 try to upgrade lock, and we have to abort txn3.
      // but there is no need to abort txn4 only if we move txn2 to front:
      //   request_queue_: (txn2,granted) (txn4,ungranted) (txn1,granted)
      // Then things will occur: txn1.unlock -> txn2.upgrade ->txn2.unlock -> txn4.sharedLock
      auto head = queue->request_queue_.begin();
      std::swap(*head, req);
    }
  }

  queue->upgrading_ = txn_id;
  queue->cv_.wait(lk, [&] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto it = queue->request_queue_.begin();
    assert(it->txn_id_ == txn_id);
    for (++it; it != queue->request_queue_.end(); ++it) {
      if (it->lock_mode_ != LockMode::SHARED) {
        break;
      }
      if (it->granted_) {
        blocking_[txn_id] = rid;
        return false;
      }
    }
    return true;
  });
  blocking_.erase(txn_id);
  queue->upgrading_ = INVALID_TXN_ID;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  auto head = queue->request_queue_.begin();
  assert(head->txn_id_ == txn_id);
  assert(head->lock_mode_ == LockMode::SHARED);
  head->lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  // LOG_DEBUG("transaction %d upgraded to exclusivs-lock", txn_id);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  // LOG_DEBUG("transaction %d Unlock %s", txn->GetTransactionId(), rid.ToString().c_str());
  std::lock_guard lk(latch_);
  LockRequestQueue *queue = &lock_table_[rid];
  auto it = queue->request_queue_.begin();
  for (; it != queue->request_queue_.end(); ++it) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  // GROWING -> SHRINKING
  if (txn->GetState() == TransactionState::GROWING) {
    if (it->lock_mode_ == LockMode::EXCLUSIVE || txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
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
  for (auto &r : request_queue_) {
    if (r.granted_) {
      return true;
    }
  }
  return false;
}

void LockManager::LockRequestQueue::Grant(txn_id_t txn_id) {
  for (auto &r : request_queue_) {
    if (r.txn_id_ == txn_id) {
      r.granted_ = true;
      return;
    }
  }
  UNREACHABLE("grant failed");
}

auto LockManager::LockRequestQueue::ToString() -> std::string {
  std::ostringstream str_stream;
  for (auto &r : request_queue_) {
    str_stream << r.ToString() << " ";
  }
  return str_stream.str();
}

auto LockManager::LockRequest::ToString() -> std::string {
  std::ostringstream str_stream;
  str_stream << "(Txn" << txn_id_ << ",x" << (lock_mode_ == LockMode::EXCLUSIVE) << ",gr" << granted_ << ",gw"
             << (txn_->GetState() == TransactionState::GROWING) << ")";
  return str_stream.str();
}

}  // namespace bustub
