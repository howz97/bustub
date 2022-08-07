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

LRUReplacer::LRUReplacer(size_t num_pages) : frame_list{std::list<frame_id_t>()}, cap{num_pages} {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(mu);
  if (frame_list.empty()) {
    return false;
  }
  *frame_id = frame_list.back();
  frame_list.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu);
  frame_list.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mu);
  if (frame_list.size() >= cap) {
    LOG_ERROR("replacer is already full");
  }
  frame_list.push_front(frame_id);
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(mu);
  return frame_list.size();
}

}  // namespace bustub
