#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

// 定义一个模板类 ExtendibleHashTable，支持通过键值对进行哈希表操作
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 初始化哈希表，设置全局深度为0，桶的大小和桶的数量为1，并创建一个初始桶
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
}

// 根据键值计算索引
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1; // 使用位掩码来获取哈希值的低 global_depth_ 位
  return std::hash<K>()(key) & mask; // 通过哈希函数计算键的索引位置
}

// 获取哈希表的全局深度
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_); // 锁保护访问
  return global_depth_;
}

// 获取指定索引处的桶的深度
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_); // 锁保护访问
  return dir_[dir_index]->GetDepth(); // 返回指定索引位置的桶的深度
}

// 获取当前哈希表中桶的数量
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_); // 锁保护访问
  return num_buckets_;
}

// 查找指定键的值，如果存在则返回 true 并将值存入参数 value 中
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_); // 锁保护访问
  size_t index = IndexOf(key); // 计算索引位置
  return dir_[index]->Find(key, value); // 在对应桶中查找
}

// 增加桶的局部深度
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Bucket::IncrementDepth() {
  depth_++; // 增加桶的深度
}

// 插入键值对到哈希表中
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(latch_); // 锁保护访问
  while (true) {
    size_t index = IndexOf(key); // 计算索引位置
    auto bucket = dir_[index]; // 获取对应索引位置的桶

    if (bucket->Insert(key, value)) {
      // 如果插入成功，退出函数
      return;
    }

    // 如果当前桶已满且其深度等于全局深度，扩展哈希表
    if (bucket->GetDepth() == global_depth_) {
      if (global_depth_ >= kMaxGlobalDepth) {
        throw std::overflow_error("Maximum global depth reached. Cannot insert more keys.");
      }
      global_depth_++; // 增加全局深度
      size_t dir_size = dir_.size();
      dir_.resize(dir_size * 2); // 扩大目录数组，双倍扩展
      for (size_t i = 0; i < dir_size; i++) {
        dir_[i + dir_size] = dir_[i]; // 复制目录内容到新的部分
      }
    }

    // 提升桶的深度并创建新桶
    bucket->IncrementDepth();
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
    num_buckets_++; // 增加桶的数量

    // 将原有桶中的所有元素根据新索引重新分配到新桶或旧桶
    auto &old_items = bucket->GetItems();
    for (auto it = old_items.begin(); it != old_items.end();) {
      size_t new_index = IndexOf(it->first); // 计算新索引
      if (new_index != index) {
        new_bucket->Insert(it->first, it->second); // 插入到新桶
        it = old_items.erase(it); // 从旧桶中移除已插入的项
      } else {
        ++it;
      }
    }

    // 更新目录中指向旧桶的条目为新桶
    auto old_bucket = bucket;
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == old_bucket && (i & (1 << (old_bucket->GetDepth() - 1)))) {
        dir_[i] = new_bucket;
      }
    }
  }
}

// 从哈希表中移除指定键的元素
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_); // 锁保护访问
  size_t index = IndexOf(key); // 计算索引位置
  return dir_[index]->Remove(key); // 在对应桶中移除
}

// 定义桶的构造函数
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t size, int depth) : size_(size), depth_(depth) {}

// 在桶中查找键值对
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false; // 找不到返回 false
}

// 从桶中移除指定的键值对
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false; // 找不到返回 false
}

// 将键值对插入到桶中
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &item : list_) {
    if (item.first == key) {
      item.second = value; // 如果已存在键，更新其值
      return true;
    }
  }

  // 如果桶已满，返回 false
  if (IsFull()) {
    return false;
  }

  list_.emplace_back(key, value); // 否则，插入新的键值对
  return true;
}

// 实例化模板类以供测试和使用
template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// 用于测试的实例化模板类
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

