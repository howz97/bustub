//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto *dir_p =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  dir_p->SetPageId(directory_page_id_);
  page_id_t bucket0_id = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bucket0_id);
  assert(bucket0_id != INVALID_PAGE_ID);
  dir_p->SetBucketPageId(0, bucket0_id);
  dir_p->SetLocalDepth(0, 0);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket0_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  if (page == nullptr) {
    // LOG_WARN("HASH_TABLE_TYPE::FetchDirectoryPage failed to FetchPage(%d)", directory_page_id_);
    return nullptr;
  }
  auto *dir_p = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_p;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  if (page == nullptr) {
    return nullptr;
  }
  auto *bkt_p = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  return bkt_p;
}

auto KeepLeastBits(uint32_t value, uint8_t nbits) -> uint32_t {
  uint32_t musk = (1 << nbits) - 1;
  return value & musk;
}

template <typename F>
void IterBuckets(uint32_t index, uint8_t gd, uint8_t ld, F lambda) {
  uint32_t first_index = KeepLeastBits(index, ld);
  uint32_t end = 1 << (gd - ld);
  for (uint32_t i = 0; i != end; ++i) {
    uint32_t bucket_idx = (i << ld) | first_index;
    lambda(bucket_idx);
  }
}

auto CheckBit(uint32_t num, uint8_t offset) -> bool {
  uint32_t musk = 1 << (offset - 1);
  return (num & musk) > 0;
}

auto InvertBit(uint32_t num, uint8_t offset) -> uint32_t {
  uint32_t musk = 1 << (offset - 1);
  return num ^ musk;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return false;
  }
  // TODO(zhanghao): consider this optimization, FetchPage -> RLock -> check page
  table_latch_.RLock();
  Page *raw_page = buffer_pool_manager_->FetchPage(KeyToPageId(key, dir_page));
  if (raw_page == nullptr) {
    // LOG_WARN("HASH_TABLE_TYPE::GetValue failed to fetch bucket %d", KeyToPageId(key, dir_page));
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  // start to scan bucket
  raw_page->RLatch();
  table_latch_.RUnlock();                                      // release lock of directory table
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);  // unpin directory page
  auto *bkt_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_page->GetData());
  bool found = bkt_page->GetValue(key, comparator_, result);
  raw_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return false;
  }
  table_latch_.RLock();
  Page *raw_page = buffer_pool_manager_->FetchPage(KeyToPageId(key, dir_page));
  if (raw_page == nullptr) {
    // LOG_WARN("HASH_TABLE_TYPE::Insert failed to FetchPage(%d)", KeyToPageId(key, dir_page));
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  // start to insert into bucket
  raw_page->WLatch();
  table_latch_.RUnlock();                                      // release lock of directory table
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);  // unpin directory page
  auto *bkt_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_page->GetData());
  uint8_t code = bkt_page->Insert2(key, value, comparator_);
  raw_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), code == CODE_OK);
  if (code == CODE_FULL) {
    return SplitInsert(transaction, key, value);
  }
  return code == CODE_OK;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return false;
  }
  table_latch_.WLock();
  Page *raw_page = buffer_pool_manager_->FetchPage(KeyToPageId(key, dir_page));
  if (raw_page == nullptr) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return false;
  }
  raw_page->WLatch();
  auto *bkt_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_page->GetData());
  // fixme: how to handle duplicated insert ?
  // if (bkt_page->Insert(key, value, comparator_)) {
  //   table_latch_.WUnlock();
  //   raw_page->WUnlatch();
  //   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  //   buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), true);
  //   return true;
  // }

  // failed to insert directly, let's split bucket
  // create new page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    table_latch_.WUnlock();
    raw_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), false);
    return false;
  }
  auto *new_bkt = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_dep = dir_page->GetLocalDepth(index);
  // increment global depth as needed
  if (dir_page->GetGlobalDepth() == local_dep) {
    dir_page->IncrGlobalDepth();
  }
  // change pointers of pages in directory
  IterBuckets(index, dir_page->GetGlobalDepth(), local_dep, [&](uint32_t i) {
    if (CheckBit(i, local_dep + 1)) {
      dir_page->SetBucketPageId(i, new_page_id);
    }
    dir_page->IncrLocalDepth(i);
  });
  // move some pairs from origin bucket to new bucket
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!bkt_page->IsOccupied(i)) {
      break;
    }
    if (!bkt_page->IsReadable(i)) {
      continue;
    }
    if (KeyToPageId(bkt_page->KeyAt(i), dir_page) == new_page_id) {
      assert(new_bkt->Insert(bkt_page->KeyAt(i), bkt_page->ValueAt(i), comparator_));
      bkt_page->RemoveAt(i);
    }
  }
  // retry to insert key-value again failed just now
  if (KeyToPageId(key, dir_page) == new_page_id) {
    assert(new_bkt->Insert(key, value, comparator_));
  } else {
    assert(bkt_page->Insert(key, value, comparator_));
  }
  table_latch_.WUnlock();
  raw_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), true);  // FIXME: maybe not dirty
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return false;
  }
  table_latch_.RLock();
  Page *raw_page = buffer_pool_manager_->FetchPage(KeyToPageId(key, dir_page));
  if (raw_page == nullptr) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  // start to remove from bucket
  raw_page->WLatch();
  auto *bkt_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_page->GetData());
  bool ok = bkt_page->Remove(key, value, comparator_);
  bool try_merge = bkt_page->IsEmpty() && dir_page->GetGlobalDepth() > 0;
  raw_page->WUnlatch();
  table_latch_.RUnlock();                                      // release lock of directory table
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);  // unpin directory page
  buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), ok);
  // bucket maybe empty before remove
  if (try_merge) {
    Merge(transaction, key, value);
  }
  return ok;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return;
  }
  table_latch_.WLock();
  Page *raw_page = buffer_pool_manager_->FetchPage(KeyToPageId(key, dir_page));
  if (raw_page == nullptr) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  raw_page->WLatch();
  auto *bkt_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(raw_page->GetData());
  bool ok = false;
  if (bkt_page->IsEmpty()) {
    uint32_t index = KeyToDirectoryIndex(key, dir_page);
    uint32_t local_dep = dir_page->GetLocalDepth(index);
    if (local_dep > 0) {
      uint32_t img_idx = InvertBit(index, local_dep);
      if (local_dep == dir_page->GetLocalDepth(img_idx)) {
        page_id_t pg_id = dir_page->GetBucketPageId(img_idx);
        IterBuckets(index, dir_page->GetGlobalDepth(), local_dep - 1, [&](uint32_t i) {
          dir_page->SetBucketPageId(i, pg_id);
          dir_page->DecrLocalDepth(i);
        });
        ok = true;
      }
    }
  }
  LOG_DEBUG("HASH_TABLE_TYPE::Merge ok=%d, canShrink=%d", ok, dir_page->CanShrink());
  dir_page->PrintDirectory();
  if (ok && dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  table_latch_.WUnlock();
  raw_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(directory_page_id_, ok);
  buffer_pool_manager_->UnpinPage(raw_page->GetPageId(), false);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDirectory() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
