//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

/**
 * Create a new LRUReplacer.
 * @param num_pages the maximum number of pages the LRUReplacer will be required to store
 */
LRUReplacer::LRUReplacer(size_t num_pages) : frame_list_{std::list<frame_id_t>()} {
  for (size_t i = 0; i != num_pages; ++i) {
    map_.emplace_back(frame_list_.end());
  }
}

/**
 * Destroys the LRUReplacer.
 */
LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(mu_);
  if (frame_list_.empty()) {
    return false;
  }
  *frame_id = frame_list_.back();
  frame_list_.pop_back();
  map_[*frame_id] = frame_list_.end();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  auto it = map_[frame_id];
  // already pined
  if (it == frame_list_.end()) {
    return;
  }
  map_[frame_id] = frame_list_.end();
  frame_list_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  if (auto it = map_[frame_id]; it != frame_list_.end()) {
    return;
  }
  frame_list_.push_front(frame_id);
  map_[frame_id] = frame_list_.begin();
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(mu_);
  return frame_list_.size();
}

}  // namespace bustub
