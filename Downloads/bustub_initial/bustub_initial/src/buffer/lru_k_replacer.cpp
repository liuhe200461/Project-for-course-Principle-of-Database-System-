#include "buffer/lru_k_replacer.h"

// 构造函数，初始化LRU-K替换器
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : replacer_size_(num_frames), k_(k) {
    max_size_ = num_frames;
}

// 驱逐一个页面
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);

    // 如果当前没有可以驱逐的页面，返回false
    if (Size() == 0) {
        return false;
    }

    // 优先尝试驱逐距离为无限大的页面（新队列中的页面）
    for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++) {
        auto frame = *it;
        if (evictable_[frame]) { // 如果该页面可以被驱逐
            recorded_cnt_[frame] = 0; // 重置访问计数
            new_locate_.erase(frame); // 从定位映射中删除
            new_frame_.remove(frame); // 从新队列中移除
            *frame_id = frame;        // 设置被驱逐的页面ID
            curr_size_--;             // 更新当前大小
            hist[frame].clear();      // 清空访问时间记录
            return true;
        }
    }

    // 尝试驱逐老队列中已经访问过k次的页面
    for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++) {
        auto frame = (*it).first;
        if (evictable_[frame]) { // 如果该页面可以被驱逐
            recorded_cnt_[frame] = 0; // 重置访问计数
            cache_frame_.erase(it);   // 从老队列中移除
            cache_locate_.erase(frame); // 从定位映射中删除
            *frame_id = frame;          // 设置被驱逐的页面ID
            curr_size_--;               // 更新当前大小
            hist[frame].clear();        // 清空访问时间记录
            return true;
        }
    }

    // 没有页面可以被驱逐，返回false
    return false;
}

// 记录页面的访问
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // 检查页面ID是否合法
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        throw std::exception();
    }

    // 更新全局时间戳并记录页面的访问次数
    current_timestamp_++;
    recorded_cnt_[frame_id]++;
    auto cnt = recorded_cnt_[frame_id];
    hist[frame_id].push_back(current_timestamp_);

    // 如果是新加入的页面
    if (cnt == 1) {
        // 如果当前容量已满，驱逐一个页面
        if (curr_size_ == max_size_) {
            frame_id_t frame;
            Evict(&frame);
        }
        evictable_[frame_id] = true;     // 设置页面为可驱逐
        curr_size_++;                    // 更新当前大小
        new_frame_.push_front(frame_id); // 加入新队列
        new_locate_[frame_id] = new_frame_.begin(); // 更新定位映射
    }

    // 如果页面的访问次数达到k次，将其移入老队列
    if (cnt == k_) {
        new_frame_.erase(new_locate_[frame_id]); // 从新队列中移除
        new_locate_.erase(frame_id);            // 删除定位信息
        auto kth_time = hist[frame_id].front(); // 获取第k次访问的时间
        k_time new_cache(frame_id, kth_time);   // 创建缓存条目
        auto it = std::upper_bound(
            cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp); // 找到插入位置
        it = cache_frame_.insert(it, new_cache);  // 插入到老队列
        cache_locate_[frame_id] = it;             // 更新定位映射
        return;
    }

    // 如果页面访问次数超过k次，更新其在老队列中的位置
    if (cnt > k_) {
        hist[frame_id].erase(hist[frame_id].begin()); // 删除最早的访问记录
        cache_frame_.erase(cache_locate_[frame_id]);  // 从老队列中移除
        auto kth_time = hist[frame_id].front();       // 获取倒数第k次的访问时间
        k_time new_cache(frame_id, kth_time);         // 创建缓存条目
        auto it = std::upper_bound(
            cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp); // 找到插入位置
        it = cache_frame_.insert(it, new_cache);       // 插入到老队列
        cache_locate_[frame_id] = it;                  // 更新定位映射
        return;
    }
    // 如果页面访问次数小于k次，不需要更新位置
}

// 设置页面是否可驱逐
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);

    // 如果页面没有记录，直接返回
    if (recorded_cnt_[frame_id] == 0) {
        return;
    }

    auto status = evictable_[frame_id];
    evictable_[frame_id] = set_evictable;

    // 更新容量大小
    if (status && !set_evictable) {
        --max_size_;
        --curr_size_;
    }
    if (!status && set_evictable) {
        ++max_size_;
        ++curr_size_;
    }
}

// 移除一个页面
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // 检查页面ID是否合法
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        throw std::exception();
    }

    auto cnt = recorded_cnt_[frame_id];
    if (cnt == 0) {
        return; // 页面未被记录，直接返回
    }

    if (!evictable_[frame_id]) {
        throw std::exception(); // 页面不可驱逐，抛出异常
    }

    // 根据页面访问次数从相应队列中移除
    if (cnt < k_) {
        new_frame_.erase(new_locate_[frame_id]);
        new_locate_.erase(frame_id);
        recorded_cnt_[frame_id] = 0;
        hist[frame_id].clear();
        curr_size_--;
    } else {
        cache_frame_.erase(cache_locate_[frame_id]);
        cache_locate_.erase(frame_id);
        recorded_cnt_[frame_id] = 0;
        hist[frame_id].clear();
        curr_size_--;
    }
}

// 返回当前替换器的大小
auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}

// 比较两个页面的时间戳
auto LRUKReplacer::CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool {
    return f1.second < f2.second;
}

