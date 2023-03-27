#pragma once

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <list>
#include <memory>
#include <sys/mman.h>
#include <Poco/Logger.h>
#include <mutex>
#include <new>
#include <stdexcept>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unistd.h>

#include <Common/HashTable/Hash.h>
#include <Common/ICachePolicy.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>

namespace DB 
{

extern const size_t UNIFIED_ALLOC_THRESHOLD;

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
}

/// Global memory arena under buddy allocator schema
class BuddyArena
{
public:
    union MemoryBlock;

    /// Node of double-linked list
    struct FreeMemoryBlock 
    {
        MemoryBlock * previous;
        MemoryBlock * next;
    };

    union MemoryBlock 
    {
        FreeMemoryBlock pointers;
        char data[0];
    };

    static BuddyArena & instance()
    {
        static BuddyArena arena;
        return arena;    
    }

    BuddyArena() = default;

    ~BuddyArena() noexcept;

    BuddyArena(const BuddyArena&) = delete;
    BuddyArena(BuddyArena&&) = delete;
    BuddyArena& operator=(const BuddyArena&) = delete;
    BuddyArena& operator=(BuddyArena&&) = delete;

    bool isValid() const;

    /// Check that the ptr is contained in the allocated memory arena
    bool isAllocated(const void * ptr) const;

    void initialize(size_t minimal_allocation_size, size_t size);

    void * malloc(size_t size, size_t align = 0);
    void free(void * buf) noexcept;

    [[nodiscard]] double getFreeSpaceRatio() const;
    [[nodiscard]] size_t getTotalSizeBytes() const;

private:
    void * arena_buffer;

    size_t number_of_levels = 0;
    size_t total_size_bytes = 0;

    size_t minimal_allocation_size_bytes = 0;

    std::atomic<size_t> free_min_blocks = 0;
    size_t min_blocks_num = 0;

    /// Meta storage 
    bool * block_status;
    uint8_t * pointers_levels;
    MemoryBlock ** free_lists;
    std::mutex * mutexes;

    [[nodiscard]] static void * allocateArena(size_t size); 
    static void deallocateArena(void * buffer, size_t size);

    size_t calculateLevel(size_t size) const; 
    size_t calculateLevel(size_t size, size_t align) const;
    size_t calculateBlockSizeOnLevel(size_t level) const;
    size_t calculateIndexInLevel(const MemoryBlock * block, size_t level) const; 
    size_t calculateIndex(const MemoryBlock * block, size_t level) const; 
    size_t calculatePointerIndex(const MemoryBlock * block) const;

    /// Calculate number of minimal blocks for the allocation on the lower level
    /// Used to calculate offset for the meta storage beforehand
    size_t calculateMinBlocksNumber(size_t size_bytes) const;

    MemoryBlock * blockFromIndexInLevel(size_t index_in_level, size_t level) const;

    size_t getPointerLevel(const MemoryBlock * block) const;
    void setPointerLevel(const MemoryBlock * block, size_t level); 

    MemoryBlock * allocateBlock(size_t level); 
    void deallocateBlock(MemoryBlock * block, size_t level); 

    char * initializeMetaStorage(); 

    std::pair<MemoryBlock *, MemoryBlock *> divideBlock(MemoryBlock * block, size_t level); 
    static MemoryBlock * mergeWithBuddy(MemoryBlock * block, MemoryBlock * buddy);
    MemoryBlock * getBuddy(MemoryBlock * block, size_t level) const; 

    bool getBlockStatus(const MemoryBlock * block, size_t level) const;
    void setBlockStatus(const MemoryBlock * block, size_t level, bool status);

    /// TODO: Remove this method
    /// Temporarly use only
    void printMemoryUsageDummy() const; 

    void addToFreeList(MemoryBlock * block, size_t level);
    bool isFreeListEmpty(size_t level) const; 
    void removeFromFreeList(MemoryBlock * block, size_t level);
};

template <typename TKey, typename HashFunction = std::hash<TKey>>
class LRUUnifiedCacheGlobal
{
public:
    using Key = TKey;

    static LRUUnifiedCacheGlobal & instance()
    {
        static LRUUnifiedCacheGlobal cache;
        return cache;    
    }

    LRUUnifiedCacheGlobal() = default;

    void initialize(size_t max_size_)
    {
        max_size = max_size_;
        is_cache_evictions_on_low_memory_enabled = false;
    }

    void initialize(size_t max_size_, 
                    double free_ram_ratio_to_start_cache_eviction_, 
                    double ram_ratio_for_cache_eviciton_amount_)
    {
        max_size = max_size_;
        free_ram_ratio_to_start_cache_eviction = free_ram_ratio_to_start_cache_eviction_;
        ram_ratio_for_cache_eviciton_amount = ram_ratio_for_cache_eviciton_amount_;
        is_cache_evictions_on_low_memory_enabled = true;
    }

    [[nodiscard]] bool isValid() const 
    {
        return max_size > 0;
    }

    [[nodiscard]] size_t getCacheWeight() const
    {
        std::lock_guard lock(mutex);
        return current_size;
    }

    [[nodiscard]] size_t getCacheCount() const
    {
        std::lock_guard lock(mutex);
        return cells.size();
    }

    template <typename TMapped>
    [[nodiscard]] size_t getCacheTypeWeight()
    {
        std::lock_guard lock(mutex);
        return current_size_by_types[typeid(TMapped)];
    }

    template <typename TMapped>
    [[nodiscard]] size_t getCacheTypeCount()
    {
        std::lock_guard lock(mutex);
        return current_count_by_types[typeid(TMapped)];
    }

    void remove(const Key & key, std::lock_guard<std::mutex> & /*cache_lock*/)
    {
        std::lock_guard lock(mutex);
        removeLocked(key);
    }

    template <typename TMapped> 
    std::shared_ptr<TMapped> get(const Key & key, std::lock_guard<std::mutex> & /*cache_lock*/)
    {
        std::lock_guard lock(mutex);

        auto it = cells.find(key);
        if (it == cells.end())
        {
            return std::shared_ptr<TMapped>();
        }

        Cell & cell = it->second;

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return std::static_pointer_cast<TMapped>(cell.value);
    }

    /// Similar to the remove function but performs deletions for all keys corresponding to the given type TMapped
    template <typename TMapped>
    void reset(std::lock_guard<std::mutex> & /*cache_lock*/)
    {
        std::lock_guard lock(mutex);

        const std::type_index type_to_reset(typeid(TMapped));

        std::vector<Key> keys_to_delete;
        for (const auto & entry : cells) 
        {
            const auto & cell = entry.second;
            if (cell.type == type_to_reset)
                keys_to_delete.push_back(entry.first);
        }

        for (const auto & key : keys_to_delete) 
            removeLocked(key);
    }

    template <typename TMapped>
    void set(const Key & key, const std::shared_ptr<TMapped> & mapped, size_t weight, std::lock_guard<std::mutex> & /*cache_lock*/)
    {
        std::lock_guard lock(mutex);
    
        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(typeid(TMapped)));

        Cell & cell = it->second;

        if (inserted)
        {
            try
            {
                cell.queue_iterator = queue.insert(queue.end(), key);
            }
            catch (...)
            {
                cells.erase(it);
                throw;
            }
        }
        else
        {
            current_size -= cell.size;
            current_size_by_types[cell.type] -= cell.size;
            --current_count_by_types[cell.type];
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = std::static_pointer_cast<void>(mapped);
        cell.size = cell.value ? weight : 0;
        current_size += cell.size;
        current_size_by_types[cell.type] += cell.size;
        ++current_count_by_types[cell.type];

        removeOverflowLocked();
    }

    /// Evict entries using LRU strategy with approximate size = weight
    size_t removeWeight(size_t weight)
    {
        std::lock_guard lock(mutex);
        return removeWeightLocked(weight);
    }

private:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    LRUQueue queue;

    struct Cell
    {
        std::shared_ptr<void> value = nullptr;
        size_t size = 0;
        std::type_index type; // is used in the reset funciton
        LRUQueueIterator queue_iterator{};

        explicit Cell(const std::type_info & type_info) : type(type_info) 
        {}
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

    /// Total weight of values.
    size_t current_size = 0;
    size_t max_size = 0;

    std::unordered_map<std::type_index, size_t> current_size_by_types;
    std::unordered_map<std::type_index, size_t> current_count_by_types;

    /// Evictions on low memory policy
    bool is_cache_evictions_on_low_memory_enabled = false;
    double free_ram_ratio_to_start_cache_eviction = 0.0;
    double ram_ratio_for_cache_eviciton_amount = 0.0;

    mutable std::mutex mutex;

    /// Remove entries with given key from the cells hashmap
    /// Require lock on the mutex before calling
    void removeLocked(const Key & key)
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        current_size_by_types[cell.type] -= cell.size;
        --current_count_by_types[cell.type];
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    void removeOverflowLocked()
    {
        /// Check local size setting 
        if (max_size != 0 && current_size > max_size) 
            removeWeightLocked(current_size - max_size);

        /// Ask global allocator for free space ratio
        auto & allocator_instance = BuddyArena::instance();
        auto free_space_ratio = allocator_instance.getFreeSpaceRatio();
        if (is_cache_evictions_on_low_memory_enabled && free_space_ratio < free_ram_ratio_to_start_cache_eviction) 
        {
            size_t weight_to_evict = static_cast<size_t>(allocator_instance.getTotalSizeBytes() * ram_ratio_for_cache_eviciton_amount);
            removeWeightLocked(weight_to_evict);
        }
    }

    size_t removeWeightLocked(size_t weight)
    {
        LOG_DEBUG(&Poco::Logger::get("UnifiedCache"), "LRUUnifiedCacheGlobal: remove weight {}", ReadableSize(weight));

        size_t current_weight_lost = 0;
        while (current_size > 0 && current_weight_lost < weight)
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("UnifiedCache"), "LRUUnifiedCacheGlobal became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;

            current_size -= cell.size;
            current_size_by_types[cell.type] -= cell.size;
            --current_count_by_types[cell.type];
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
        }

        return current_weight_lost;
    }
};

/// Proxy-class - redirects all calls to the corresponding methods of the LRUUnifiedCacheGlobal global instance
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class LRUUnifiedCachePolicy : public ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    using Base = ICachePolicy<TKey, TMapped, HashFunction, WeightFunction>;
    using typename Base::OnWeightLossFunction;

    using GlobalCachePolicy = LRUUnifiedCacheGlobal<TKey, HashFunction>;

    /** Initialize LRUCachePolicy with max_size and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    explicit LRUUnifiedCachePolicy(size_t max_size_, size_t max_elements_size_ = 0, OnWeightLossFunction on_weight_loss_function_ = {})
        : max_size(std::max(static_cast<size_t>(1), max_size_))
        , max_elements_size(max_elements_size_)
        , instance(GlobalCachePolicy::instance())
    {
        Base::on_weight_loss_function = on_weight_loss_function_;
    }

    size_t weight(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return instance.template getCacheTypeWeight<Mapped>();
    }

    size_t count(std::lock_guard<std::mutex> & /* cache_lock */) const override
    {
        return instance.template getCacheTypeCount<Mapped>();
    }

    size_t maxSize() const override
    {
        return max_size;
    }

    void reset(std::lock_guard<std::mutex> & cache_lock) override
    {
        instance.template reset<Mapped>(cache_lock);
    }

    void remove(const Key & key, std::lock_guard<std::mutex> & cache_lock) override
    {
        instance.remove(key, cache_lock);
    }

    MappedPtr get(const Key & key, std::lock_guard<std::mutex> & cache_lock) override
    {
        return instance.template get<Mapped>(key, cache_lock);
    }

    void set(const Key & key, const MappedPtr & mapped, std::lock_guard<std::mutex> & cache_lock) override
    {
        size_t weight = weight_function(*mapped);
        instance.template set<Mapped>(key, mapped, weight, cache_lock);
    }

protected:
    // Total weight of values.
    // Not used for now, as we have a simple global cache policy
    // TODO: Remove it
    size_t current_size = 0;
    size_t number_of_elements = 0;

    const size_t max_size;
    const size_t max_elements_size;

    WeightFunction weight_function;
    GlobalCachePolicy & instance;
};

class BuddyAllocator
{
public:
    /// Allocate memory range.
    static void * alloc(size_t size, size_t alignment = 0);

    /// Free memory range.
    static void free(void * buf, size_t size);

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    static void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0);

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = true;

private:
    static void checkSize(size_t size);
    static void * allocNoTrack(size_t size, size_t alignment = 0);
    static void freeNoTrack(void * buf, size_t size);
};

}
