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
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
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
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  if (page == nullptr) {
    return nullptr;
  }
  HashTableDirectoryPage *dir_p = reinterpret_cast<HashTableDirectoryPage *>(page->data_);
  return dir_p;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  if (page == nullptr) {
    return nullptr;
  }
  HASH_TABLE_BUCKET_TYPE *bkt_p = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->data_);
  return bkt_p;
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
  HASH_TABLE_BUCKET_TYPE *bkt_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (bkt_page == nullptr) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    return false;
  }
  bool found = bkt_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bkt_page->GetPageId(), false);
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
  HASH_TABLE_BUCKET_TYPE *bkt_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (bkt_page == nullptr) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    return false;
  }
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  bool ok = bkt_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(bkt_page->GetPageId(), ok);
  return ok;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (dir_page == nullptr) {
    return false;
  }
  HASH_TABLE_BUCKET_TYPE *bkt_page = FetchBucketPage(KeyToPageId(key, dir_page));
  if (bkt_page == nullptr) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    return false;
  }
  // fixme: how to handle duplicated insert ?
  if (bkt_page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bkt_page->GetPageId(), true);
    return true;
  }

  // failed to insert directly, let's split bucket
  // create new page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_bkt_id);
  if (new_page == nullptr) {
    return false;
  }
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_dep = dir_page->GetLocalDepth(index);
  // increment global depth as needed
  if (dir_page->GetGlobalDepth() == local_dep) {
    dir_page->IncrGlobalDepth();
  }
  // change pointers of pages in directory
  uint32_t first_index = KeepLeastBits(index, local_dep);
  uint32_t end = 1 << GetGlobalDepth() - local_dep;
  for (uint32_t i = 0; i != end; ++i) {
    uint32_t bucket_idx = (i << local_dep) | first_index;
    if (i & 1 == 1) {
      dir_page->SetBucketPageId(bucket_idx, new_page_id);
    }
    dir_page->IncrLocalDepth(bucket_idx);
  }
  // move some pairs from origin bucket to new bucket
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }
    if (KeyToPageId(bkt_page->KeyAt(i), dir_page) == new_page_id) {
    }
  }
  // retry to insert key-value again failed just now

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(bkt_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return ok;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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

auto KeepLeastBits(uint32_t value, uint8_t nbits) -> uint32_t {
  uint8_t x = 32 - nbits;
  value <<= x;
  value >>= x;
  return value;
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
