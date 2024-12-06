#ifndef LRU_K_H
#define LRU_K_H

#include <mutex>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <limits>
#include <list>

#include "common/config.h"
#include "common/macros.h"

class  LRUKReplacer {
public:
    using frame_id_t = int;
    explicit LRUKReplacer(size_t num_frames, size_t k);
    ~LRUKReplacer()=default;


    auto Evict(frame_id_t *frame_id) -> bool;

    void RecordAccess(frame_id_t frame_id);

    void SetEvictable(frame_id_t frame_id, bool set_evictable);

    void Remove(frame_id_t frame_id);

    auto Size() -> size_t;

private:
    size_t current_timestamp_{0};
    size_t curr_size_{0};
    size_t max_size_;
    size_t replacer_size_;
    size_t k_;
    std::mutex latch_;

    using timestamp = std::list<size_t>;//记录单个页时间戳的列表
    using k_time = std::pair<frame_id_t,size_t>;
    std::unordered_map<frame_id_t,timestamp> hist;//用于记录所有页的时间戳
    std::unordered_map<frame_id_t,size_t> recorded_cnt_;//用于记录,访问了多少次
    std::unordered_map<frame_id_t,bool> evictable_;//用于记录是否可以被驱逐

    std::list<frame_id_t> new_frame_;//用于记录不满k次的页
    std::unordered_map<frame_id_t,std::list<frame_id_t>::iterator> new_locate_;

    std::list<k_time> cache_frame_;//用于记录到达k次的页
    std::unordered_map<frame_id_t,std::list<k_time>::iterator> cache_locate_;
    static auto CmpTimestamp(const k_time &f1,const k_time &f2) -> bool;
};
#endif //LRU_K_H

