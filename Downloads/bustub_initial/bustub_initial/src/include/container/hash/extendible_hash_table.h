#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"  // 引入已有的基类

namespace bustub {

template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
 static constexpr int kMaxGlobalDepth = 20;  // 设置全局深度的最大值
 
  explicit ExtendibleHashTable(size_t bucket_size);

  auto GetGlobalDepth() const -> int;

  auto GetLocalDepth(int dir_index) const -> int;

  auto GetNumBuckets() const -> int;

  auto Find(const K &key, V &value) -> bool override;

  void Insert(const K &key, const V &value) override;

  auto Remove(const K &key) -> bool override;

  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    auto Find(const K &key, V &value) -> bool;

    auto Remove(const K &key) -> bool;

    auto Insert(const K &key, const V &value) -> bool;

    auto IsFull() const -> bool { return list_.size() == size_; }

    auto GetDepth() const -> int { return depth_; }

    void IncrementDepth() { depth_++; }

    auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

   private:
    size_t size_;
    int depth_;
    std::list<std::pair<K, V>> list_;
  };

 private:
  int global_depth_;
  size_t bucket_size_;
  int num_buckets_;
  mutable std::mutex latch_;
  std::vector<std::shared_ptr<Bucket>> dir_;

  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;

  auto GetLocalDepthInternal(int dir_index) const -> int;

  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub

