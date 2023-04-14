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
#include <memory>
#include <new>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 不确定这个txn需不需要加锁
  // txn->lockTxn();
  if (!CanTxnTakeLock(txn, lock_mode)) {
    // txn->UnlockTxn();
    return false;
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lcq = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lcq_lck(lcq->latch_);

  bool is_need_wait = false;
  LockRequest *upgrade_lr;
  // 检查锁，遍历链表，如果没有冲突的那就加上，否则开始等待
  for (auto *lr : lcq->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      // 如果已经持有请求的锁，返回true
      if (lr->lock_mode_ == lock_mode) {
        // txn->UnlockTxn();
        return true;
      }
      // 如果可以升级的话，尝试升级
      if (lcq->upgrading_ == INVALID_TXN_ID && CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        BookDelTableLockInfoTxn(txn, lr->lock_mode_, oid);
        lcq->upgrading_ = txn->GetTransactionId();
        lr->lock_mode_ = lock_mode;
        lr->granted_ = false;
        upgrade_lr = lr;
      } else {
        txn->SetState(TransactionState::ABORTED);
        // txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
    }
    if (!is_need_wait && lr->granted_ && !AreLocksCompatible(lr->lock_mode_, lock_mode)) {
      is_need_wait = true;
    }
  }

  if (lcq->upgrading_ != txn->GetTransactionId()) {
    upgrade_lr = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    lcq->request_queue_.push_front(upgrade_lr);
  }

  if (!is_need_wait) {
    upgrade_lr->granted_ = true;
    BookNewTableLockInfoTxn(txn, lock_mode, oid);
    // txn->UnlockTxn();
    return true;
  }

  while (!upgrade_lr->granted_) { lcq->cv_.wait(lcq_lck); }
  if (lcq->upgrading_ == txn->GetTransactionId()) {
    lcq->upgrading_ = INVALID_TXN_ID;
  }
  BookNewTableLockInfoTxn(txn, lock_mode, oid);
  // txn->UnlockTxn();
  return true; 
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // txn->lockTxn();

  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    // txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  table_lock_map_latch_.lock();
  auto lcq = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  
  std::unique_lock<std::mutex> lcq_lck(lcq->latch_);
  bool is_deleted = false;
  for (auto *lr : lcq->request_queue_) {
    if (lr->txn_id_ != txn->GetTransactionId()) { continue ; }
    if (!lr->granted_) {
      txn->SetState(TransactionState::ABORTED);
      // txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    BookDelTableLockInfoTxn(txn, lr->lock_mode_, oid);
    lcq->request_queue_.remove(lr);
    UpdateTxnStateByUnlock(txn, lr->lock_mode_);
    
    delete lr;
    is_deleted = true;
    break ;
  }
  if (!is_deleted) {
    txn->SetState(TransactionState::ABORTED);
    // txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  GrantNewLocksIfPossible(lcq.get());
  // txn->UnlockTxn();
  lcq->cv_.notify_all();
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    return txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid);
  }
  if (row_lock_mode == LockMode::SHARED) {
    return CheckAppropriateLockOnTable(txn, oid, LockMode::EXCLUSIVE) || txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid);
  }
  LOG_ERROR("Lock Mode not equl x or s, txn id : %d", txn->GetTransactionId());
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 不确定这个txn需不需要加锁
  // txn->lockTxn();

  if (!CanTxnTakeLock(txn, lock_mode)) {
    // txn->UnlockTxn();
    return false;
  }
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    // txn->UnlockTxn();
    return false;
  }

  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    // txn->UnlockTxn();
    return false;
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lcq = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lcq_lck(lcq->latch_);

  bool is_need_wait = false;
  LockRequest *upgrade_lr;
  // 检查锁，遍历链表，如果没有冲突的那就加上，否则开始等待
  for (auto *lr : lcq->request_queue_) {
    if (lr->txn_id_ == txn->GetTransactionId()) {
      // 如果已经持有请求的锁，返回true
      if (lr->lock_mode_ == lock_mode) {
        // txn->UnlockTxn();
        return true;
      }
      // 如果可以升级的话，尝试升级
      if (lcq->upgrading_ == INVALID_TXN_ID && CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        BookDelRowLockInfoTxn(txn, lr->lock_mode_, oid, rid);
        lcq->upgrading_ = txn->GetTransactionId();
        lr->lock_mode_ = lock_mode;
        lr->granted_ = false;
        upgrade_lr = lr;
      } else {
        txn->SetState(TransactionState::ABORTED);
        // txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
    }
    if (!is_need_wait && lr->granted_ && !AreLocksCompatible(lr->lock_mode_, lock_mode)) {
      is_need_wait = true;
    }
  }

  if (lcq->upgrading_ != txn->GetTransactionId()) {
    upgrade_lr = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    lcq->request_queue_.push_front(upgrade_lr);
  }

  if (!is_need_wait) {
    upgrade_lr->granted_ = true;
    BookNewRowLockInfoTxn(txn, lock_mode, oid, rid);
    // txn->UnlockTxn();
    return true;
  }

  while (!upgrade_lr->granted_) { lcq->cv_.wait(lcq_lck); }
  if (lcq->upgrading_ == txn->GetTransactionId()) {
    lcq->upgrading_ = INVALID_TXN_ID;
  }
  BookNewRowLockInfoTxn(txn, lock_mode, oid, rid);
  // txn->UnlockTxn();
  return true; 
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // // txn->lockTxn();

  row_lock_map_latch_.lock();
  auto lcq = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lcq_lck(lcq->latch_);
  bool is_deleted = false;
  for (auto *lr : lcq->request_queue_) {
    if (lr->txn_id_ != txn->GetTransactionId()) { continue ; }
    if (!lr->granted_) {
      txn->SetState(TransactionState::ABORTED);
      // // txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    BookDelRowLockInfoTxn(txn, lr->lock_mode_, oid, rid);
    lcq->request_queue_.remove(lr);
    if (!force) {
      UpdateTxnStateByUnlock(txn, lr->lock_mode_);
    }
    
    delete lr;
    is_deleted = true;
    break ;
  }
  if (!is_deleted) {
    txn->SetState(TransactionState::ABORTED);
    // // txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  GrantNewLocksIfPossible(lcq.get());
  // // txn->UnlockTxn();
  lcq->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::UpdateTxnStateByUnlock(Transaction *txn, LockMode lock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED) {
        LOG_ERROR("Unlock S lock under READ_UNCOMMITTED level. tid : %d", txn->GetTransactionId());
      }
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
  }
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
  std::vector<LockMode> locks;
  for (auto *lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      locks.push_back(lr->lock_mode_);
    }
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    for (auto *lr : lock_request_queue->request_queue_) {
      if (lr->granted_ || lock_request_queue->upgrading_ != lr->txn_id_) {
        continue;
      }
      bool is_compatible = true;
      for (auto &lcm : locks) {
        if (!AreLocksCompatible(lcm, lr->lock_mode_)) {
          is_compatible = false;
          break ;
        }
      }
      // TODO(blickwinkle): 这里可以优化，如果其余的请求和upgrade的请求不冲突的话，可以grant给其他请求的。
      if (!is_compatible) {
        return ;
      }
      lr->granted_ = true;
      locks.push_back(lr->lock_mode_);
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      break ;
    }
  }
  for (auto *lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      continue ;
    }
    
    bool is_compatible = true;
    for (auto &lcm : locks) {
      if (!AreLocksCompatible(lcm, lr->lock_mode_)) {
        is_compatible = false;
        break ;
      }
    }
    if (!is_compatible) {
      continue ;
    }
    lr->granted_ = true;
    locks.push_back(lr->lock_mode_);
  }
  
}

void LockManager::BookNewTableLockInfoTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

void LockManager::BookDelTableLockInfoTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}
void LockManager::BookDelRowLockInfoTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      LOG_ERROR("UnLock Mode not equl x or s, txn id : %d", txn->GetTransactionId());
  }
}

void LockManager::BookNewRowLockInfoTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      LOG_ERROR("Lock Mode not equl x or s, txn id : %d", txn->GetTransactionId());
  }
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      if (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED) {
        return true;
      }
      break ;
    case LockMode::EXCLUSIVE:
      break ;
    case LockMode::INTENTION_SHARED:
      if (l2 != LockMode::EXCLUSIVE) {
        return true;
      }
      break ;
    case LockMode::INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE) {
        return true;
      }
      break ;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (l2 == LockMode::INTENTION_SHARED) {
        return true;
      }
      break;
  }
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  // 如果交易已经被终止，那么不会给它分配锁
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 根据锁的模式、txn目前阶段以及隔离等级，识别不合法的请求
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED :
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      break;
    case IsolationLevel::READ_COMMITTED :
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ :
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      break;
  };
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::SHARED:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break ;
    case LockMode::EXCLUSIVE:
      break ;
    case LockMode::INTENTION_SHARED:
      if (requested_lock_mode != LockMode::INTENTION_SHARED) {
        return true;
      }
      break ;
    case LockMode::INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break ;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
  }
  return false;
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_lck(waits_for_latch_);
  for ()
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
