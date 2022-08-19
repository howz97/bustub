//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

// Allocate and create individual BufferPoolManagerInstances
ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : start_index_(0), num_ins_(num_instances) {
  for (size_t i = 0; i != num_instances; ++i) {
    instances_.emplace_back(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

// Get size of all BufferPoolManagerInstances
auto ParallelBufferPoolManager::GetPoolSize() -> size_t { return instances_.size() * instances_[0].GetPoolSize(); }

// Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  return &instances_[page_id % instances_.size()];
}

// Fetch page for page_id from responsible BufferPoolManagerInstance
auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  BufferPoolManager *mgr = GetBufferPoolManager(page_id);
  return mgr->FetchPage(page_id);
}

// Unpin page_id from responsible BufferPoolManagerInstance
auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  BufferPoolManager *mgr = GetBufferPoolManager(page_id);
  return mgr->UnpinPage(page_id, is_dirty);
}

// Flush page_id from responsible BufferPoolManagerInstance
auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  BufferPoolManager *mgr = GetBufferPoolManager(page_id);
  return mgr->FlushPage(page_id);
}

// create new page. We will request page allocation in a round robin manner from the underlying
// BufferPoolManagerInstances
// 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
// starting index and return nullptr
// 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
// is called
auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  if (num_ins_ == 0) {
    return nullptr;
  }
  for (size_t i = 0; i != num_ins_; ++i) {
    BufferPoolManager *mgr = &instances_[(start_index_ + i) % num_ins_];
    Page *page = mgr->NewPage(page_id);
    if (page != nullptr) {
      start_index_++;
      start_index_ %= num_ins_;
      return page;
    }
  }
  start_index_++;
  start_index_ %= num_ins_;
  return nullptr;
}

// Delete page_id from responsible BufferPoolManagerInstance
auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  BufferPoolManager *mgr = GetBufferPoolManager(page_id);
  return mgr->DeletePage(page_id);
}

// flush all pages from all BufferPoolManagerInstances
void ParallelBufferPoolManager::FlushAllPgsImp() {
  for (size_t i = 0; i != instances_.size(); ++i) {
    BufferPoolManager *mgr = &instances_[i];
    mgr->FlushAllPages();
  }
}

}  // namespace bustub
