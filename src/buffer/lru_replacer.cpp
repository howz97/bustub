//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : frame_list_{std::list<frame_id_t>()}, cap_{num_pages} {
  map_ = std::vector<std::list<frame_id_t>::iterator *>(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(mu_);
  if (frame_list_.empty()) {
    return false;
  }
  *frame_id = frame_list_.back();
  frame_list_.pop_back();
  map_[*frame_id] = nullptr;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  std::list<frame_id_t>::iterator *it = map_[frame_id];
  // already pined
  if (it == nullptr) {
    return;
  };
  map_[frame_id] = nullptr;
  frame_list_.erase(*it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu_);
  if (frame_list_.size() >= cap_) {
    LOG_ERROR("replacer is already full");
  }
  if (std::list<frame_id_t>::iterator *it = map_[frame_id]; it != nullptr) {
    map_[frame_id] = nullptr;
    frame_list_.erase(*it);
  };
  frame_list_.push_front(frame_id);
  map_[frame_id] = &frame_list_.begin();
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(mu_);
  return frame_list_.size();
}

}  // namespace bustub
