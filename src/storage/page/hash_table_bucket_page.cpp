//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool found = false;
  for (size_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      result->push_back(array_[i].second);
      found = true;
    }
  }
  return found;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> uint8_t {
  uint32_t tombstone = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      if (tombstone == BUCKET_ARRAY_SIZE) {
        SetOccupied(i);
        tombstone = i;
      }
      break;
    }
    if (!IsReadable(i)) {
      tombstone = i;
      continue;
    }
    // key-value already exist
    if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      LOG_ERROR("HASH_TABLE_BUCKET_TYPE::Insert duplicated key-value");
      return CODE_DUP;
    }
  }
  // bucket is full
  if (tombstone == BUCKET_ARRAY_SIZE) {
    LOG_WARN("HASH_TABLE_BUCKET_TYPE::Insert bucket is full");
    return CODE_FULL;
  }
  array_[tombstone] = MappingType(key, value);
  SetReadable(tombstone);
  return CODE_OK;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (!IsReadable(i)) {
      continue;
    }
    if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] &= ~uint8_t(128 >> (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  return (occupied_[bucket_idx / 8] & uint8_t(128 >> (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= uint8_t(128 >> (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  return (readable_[bucket_idx / 8] & uint8_t(128 >> (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= uint8_t(128 >> (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  return IsOccupied(BUCKET_ARRAY_SIZE - 1) && NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t cnt = 0;
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      ++cnt;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  for (uint32_t i = 0; i != BUCKET_ARRAY_SIZE; ++i) {
    if (occupied_[i] == 0) {
      break;
    }
    if (readable_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
