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

    ~BuddyArena() noexcept
    {
        if (isValid())
            // Do not throw exceptions from invalid munmap
            munmap(arena_buffer, total_size_bytes);
    }

    BuddyArena(const BuddyArena&) = delete;
    BuddyArena(BuddyArena&&) = delete;
    BuddyArena& operator=(const BuddyArena&) = delete;
    BuddyArena& operator=(BuddyArena&&) = delete;

    bool isValid() const 
    {
        return number_of_levels != 0;
    }

    /// Check that the ptr is contained in the allocated memory arena
    bool isAllocated(const void * ptr) const 
    {
        const auto * arena_end = reinterpret_cast<char *>(arena_buffer) + total_size_bytes;
        return ptr >= arena_buffer && ptr < reinterpret_cast<const void *>(arena_end); 
    }

    void initialize(size_t minimal_allocation_size, size_t size)
    {
        /// We will divide the whole size bytes on blocks with size == minimal_allocation_size
        assert(size % minimal_allocation_size == 0);

        total_size_bytes = size;
        number_of_levels = 1;
        minimal_allocation_size_bytes = minimal_allocation_size;

        /// Calculate number of levels
        size_t number_of_blocks_on_level = size / minimal_allocation_size; 

        /// Number of blocks on the level could not be the power of 2
        /// In this case we intentionally add one level to the binary tree so we'll have enough 
        /// leaves to store all minimal blocks
        if (number_of_blocks_on_level > 1 && number_of_blocks_on_level % 2 != 0) 
            ++number_of_levels;

        while (number_of_blocks_on_level > 1) 
        {
            number_of_blocks_on_level >>= 1;
            ++number_of_levels;
        }

        /// Calculate number of minimal blocks in the memory arena
        min_blocks_num = (1ull << (number_of_levels - 1));
        free_min_blocks = min_blocks_num;

        arena_buffer = allocateArena(total_size_bytes);

        auto * current_storage_ptr = initializeMetaStorage();


        // TODO: Optimize
        /// Initialize free_lists

        /// Calculate number of blocks for the meta storage
        size_t meta_storage_size_minimal_blocks = (current_storage_ptr - reinterpret_cast<char *>(arena_buffer)) / minimal_allocation_size_bytes;

        /// Calculate the nearest power of 2 that is greater than meta storage size
        size_t meta_storage_size_round_up_to_power_of_2 = 1;
        while (meta_storage_size_round_up_to_power_of_2 < meta_storage_size_minimal_blocks) 
        {
            meta_storage_size_round_up_to_power_of_2 *= 2;
        }

        /// Deallocate minimal blocks to fill the space between the meta storage and the size that is
        /// the nearest power of 2, so after it we can deallocate blocks with an exponential increasing sizes 
        /// Arena buffer:
        /// [*******-------------|------------------------------------]
        ///  |               |   |
        ///  ^ meta storage  |   ^ meta_storage_size_round_up_to_power_of_2
        ///                  ^ deallocate these blocks on this step
        const auto * minimal_blocks_area_end = reinterpret_cast<char *>(arena_buffer) + meta_storage_size_round_up_to_power_of_2 * minimal_allocation_size_bytes;
        while (current_storage_ptr != minimal_blocks_area_end) 
        {
            deallocateBlock(reinterpret_cast<MemoryBlock *>(current_storage_ptr), number_of_levels - 1);
            current_storage_ptr += minimal_allocation_size_bytes;
        }

        /// Deallocate all next blocks after meta_storage_size_round_up_to_power_of_2 with increasing sizes 
        size_t current_block_size_bytes = meta_storage_size_round_up_to_power_of_2 * minimal_allocation_size;
        size_t current_block_level = calculateLevel(current_block_size_bytes);
        while (current_block_level > 0) 
        {
            deallocateBlock(reinterpret_cast<MemoryBlock *>(current_storage_ptr), current_block_level);
            current_storage_ptr += current_block_size_bytes;
            current_block_size_bytes *= 2;
            --current_block_level;
        }
    }

    void * malloc(size_t size, size_t align = 0) 
    {
        /// TODO: Add assertion
        /// assert that align is a power of 2 and a multiple of sizeof(void*) 
        /// https://en.cppreference.com/w/cpp/memory/c/aligned_alloc

        auto level = calculateLevel(size, align);

        std::lock_guard lock(mutex);
        auto * block = allocateBlock(level);
        setPointerLevel(block, level);

        /// TODO: Remove temp local dummy memory tracker
        const size_t allocated_memory_blocks = 1ull << ((number_of_levels - level) - 1);
        free_min_blocks.fetch_sub(allocated_memory_blocks);
        // if (free_min_blocks % 10000 == 0) {
        //     printMemoryUsageDummy();
        // }

        return block->data;
    }

    /// TODO: Probably remove this function, code duplication
    /// This function could be implemented faster than free(void *) but the optimization can be minor
    void free(void * buf, size_t size) noexcept
    {
        auto * block = reinterpret_cast<MemoryBlock *>(buf);
        auto level = calculateLevel(size);

        std::lock_guard lock(mutex);
        deallocateBlock(block, level);

        /// TODO: Remove temp local dummy memory tracker
        const size_t freeing_memory_blocks = 1ull << ((number_of_levels - level) - 1);
        free_min_blocks.fetch_add(freeing_memory_blocks);
        // if (free_min_blocks % 10000 == 0) {
        //     printMemoryUsageDummy();
        // }
    }

    void free(void * buf) noexcept
    {
        auto * block = reinterpret_cast<MemoryBlock *>(buf);
        auto level = getPointerLevel(block);

        std::lock_guard lock(mutex);
        deallocateBlock(block, level);

        /// TODO: Remove temp local dummy memory tracker
        const size_t freeing_memory_blocks = 1ull << ((number_of_levels - level) - 1);
        free_min_blocks.fetch_add(freeing_memory_blocks);
        // if (free_min_blocks % 10000 == 0) {
        //     printMemoryUsageDummy();
        // }
    }

    [[nodiscard]] double getFreeSpaceRatio() const
    {
        /// TODO: Change seq_cst memory_order on the free_min_blocks atomic
        auto occupied_blocks = min_blocks_num - free_min_blocks.load();
        return static_cast<double>(min_blocks_num - occupied_blocks) / min_blocks_num;
    }

    [[nodiscard]] size_t getTotalSizeBytes() const
    {
        return total_size_bytes;
    }

private:
    void * arena_buffer;

    size_t number_of_levels = 0;
    size_t total_size_bytes = 0;

    size_t minimal_allocation_size_bytes = 0;

    std::atomic<size_t> free_min_blocks = 0;
    size_t min_blocks_num = 0;

    std::mutex mutex;

    /// Meta storage 
    bool * block_status;
    uint8_t * pointers_levels;
    MemoryBlock ** free_lists;

    [[nodiscard]] static void * allocateArena(size_t size) 
    {
        void * buffer = mmap(nullptr, size, PROT_READ | PROT_WRITE, 
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, /*fd=*/ -1, /*offset=*/ 0);
        if (MAP_FAILED == buffer)
            DB::throwFromErrno(fmt::format("BuddyArena: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        /// TODO: Remove temporary hack to make the memory tracker works
        /// As we allocated the memory arena, we don't want to take it into account in the 
        /// memory tracker
        // CurrentMemoryTracker::free(size);

        return buffer;
    }

    static void deallocateArena(void * buffer, size_t size) 
    {
        munmap(buffer, size);
    }

    size_t calculateLevel(size_t size) const 
    {
        // TODO: optimize
        size_t current_level = number_of_levels - 1;
        size_t current_block_size = calculateBlockSizeOnLevel(current_level);

        while (size > current_block_size) 
        {
            // Low in memory
            if (current_level == 0) 
                DB::throwFromErrno(fmt::format("BuddyArena: Cannot find level for size {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            --current_level;
            current_block_size *= 2;
        }

        return current_level;
    }

    size_t calculateLevel(size_t size, size_t align) const 
    {
        if (align < size) 
            /// Will be automatically aligned if align is valid (as it will be power of 2 that less than size)
            return calculateLevel(size);
        else 
            return calculateLevel(align);
    }

    size_t calculateBlockSizeOnLevel(size_t level) const 
    {
        return minimal_allocation_size_bytes * (1ull << ((number_of_levels - level) - 1));
    }

    size_t calculateIndexInLevel(const MemoryBlock * block, size_t level) const 
    {
        const auto * block_data = reinterpret_cast<const char *>(block);
        const auto * arena_data = static_cast<const char *>(arena_buffer);
        return (block_data - arena_data) / calculateBlockSizeOnLevel(level);
    }

    size_t calculateIndex(const MemoryBlock * block, size_t level) const 
    {
        return (1ull << level) + calculateIndexInLevel(block, level) - 1ull;
    }

    size_t calculatePointerIndex(const MemoryBlock * block) const 
    {
        const auto * block_data = reinterpret_cast<const char *>(block);
        const auto * arena_data = static_cast<const char *>(arena_buffer);
        
        return (block_data - arena_data) / calculateBlockSizeOnLevel(number_of_levels - 1);
    }

    MemoryBlock * blockFromIndexInLevel(size_t index_in_level, size_t level) const 
    {
        auto block_size = calculateBlockSizeOnLevel(level); 
        return reinterpret_cast<MemoryBlock *>(static_cast<char *>(arena_buffer) + block_size * index_in_level);
    }

    /// Calculate number of minimal blocks for the allocation on the lower level
    /// Used to calculate offset for the meta storage beforehand
    size_t calculateMinBlocksNumber(size_t size_bytes) const
    {
        return size_bytes / minimal_allocation_size_bytes + (size_bytes % minimal_allocation_size_bytes == 0 ? 0 : 1);
    }

    size_t getPointerLevel(const MemoryBlock * block) const 
    {
        const auto pointer_index = calculatePointerIndex(block);
        return pointers_levels[pointer_index];
    }

    void setPointerLevel(const MemoryBlock * block, size_t level) 
    {
        const auto pointer_index = calculatePointerIndex(block);
        pointers_levels[pointer_index] = level;
    }

    MemoryBlock * allocateBlock(size_t level) 
    {
        // Found free block in the list
        if (!isFreeListEmpty(level))
        {
            auto * free_block = free_lists[level];
            removeFromFreeList(free_block, level);
            return free_block;
        }

        // There are no free blocks on this level, allocate one from the top

        // Already on the top
        if (level == 0) 
            throw std::bad_alloc();

        auto * bigger_block = allocateBlock(level - 1);
        auto [block, buddy_block] = divideBlock(bigger_block, level - 1);
        addToFreeList(buddy_block, level);

        return block;
    }

    char * initializeMetaStorage() 
    {
        // Calculate sizes
        size_t block_status_size = (1ull << number_of_levels) - 1ull;
        size_t block_status_size_bytes = sizeof(bool) * block_status_size;
        size_t block_status_size_minimal_blocks = calculateMinBlocksNumber(block_status_size_bytes);

        size_t free_lists_size_bytes = sizeof(MemoryBlock *) * number_of_levels;
        size_t free_lists_size_minimal_blocks = calculateMinBlocksNumber(free_lists_size_bytes);

        size_t pointers_levels_size = (1ull << (number_of_levels - 1));
        size_t pointers_levels_size_bytes = sizeof(uint8_t) * pointers_levels_size;
        size_t pointers_levels_size_minmal_blocks = calculateMinBlocksNumber(pointers_levels_size_bytes);
        
        // Populate pointers 
        auto * current_storage_ptr = reinterpret_cast<char *>(arena_buffer);

        block_status = reinterpret_cast<bool *>(current_storage_ptr);
        current_storage_ptr += block_status_size_minimal_blocks * minimal_allocation_size_bytes;

        free_lists = reinterpret_cast<MemoryBlock **>(current_storage_ptr);
        current_storage_ptr += free_lists_size_minimal_blocks * minimal_allocation_size_bytes;

        pointers_levels = reinterpret_cast<uint8_t *>(current_storage_ptr);
        current_storage_ptr += pointers_levels_size_minmal_blocks * minimal_allocation_size_bytes;

        return current_storage_ptr;
    }

    void deallocateBlock(MemoryBlock * block, size_t level) 
    {
        assert(block);

        auto * buddy = getBuddy(block, level);

        if (buddy && getBlockStatus(buddy, level)) 
        {
            // Merge with buddy
            auto * merged_block = mergeWithBuddy(block, buddy);
            removeFromFreeList(buddy, level);
            deallocateBlock(merged_block, level - 1);
        } else 
        {
            addToFreeList(block, level);
        }  
    }

    std::pair<MemoryBlock *, MemoryBlock *> divideBlock(
        MemoryBlock * block, size_t level) 
    {
        auto block_size = calculateBlockSizeOnLevel(level);

        // TODO: Can be optimized
        auto * block_data = reinterpret_cast<char *>(block);
        auto * buddy_block_data = block_data + block_size / 2;

        auto * memory_block = reinterpret_cast<MemoryBlock *>(block_data);
        auto * buddy_memory_block = reinterpret_cast<MemoryBlock *>(buddy_block_data);

        return {memory_block, buddy_memory_block};
    }

    static MemoryBlock * mergeWithBuddy(MemoryBlock * block, MemoryBlock * buddy)
    {
        assert(block);
        assert(buddy);

        auto * block_data = reinterpret_cast<char *>(block);
        auto * buddy_block_data = reinterpret_cast<char *>(buddy);

        if (block_data < buddy_block_data) 
            return reinterpret_cast<MemoryBlock *>(block_data);
        else 
            return reinterpret_cast<MemoryBlock *>(buddy_block_data);
    }

    MemoryBlock * getBuddy(MemoryBlock * block, size_t level) const 
    {
        auto index_in_level = calculateIndexInLevel(block, level);
        size_t buddy_index_in_level = index_in_level % 2 == 0 ? index_in_level + 1 : index_in_level - 1;
        return blockFromIndexInLevel(buddy_index_in_level, level);
    }

    bool getBlockStatus(const MemoryBlock * block, size_t level)
    {
        auto index_of_block = calculateIndex(block, level);
        return block_status[index_of_block];
    }

    void setBlockStatus(const MemoryBlock * block, size_t level, bool status)
    {
        auto index_of_block = calculateIndex(block, level);
        block_status[index_of_block] = status; 
    }

    /// TODO: Remove this method
    /// Temporarly use only
    void printMemoryUsageDummy() const 
    {
        auto occupied_blocks = min_blocks_num - free_min_blocks;
        double usage = static_cast<double>(occupied_blocks) / min_blocks_num * 100.0;
        std::cout << "***Memory usage: " << occupied_blocks << " / " << min_blocks_num << " (" << std::fixed << std::setprecision(1) << usage << " %)***" << std::endl;
    }

    void addToFreeList(MemoryBlock * block, size_t level) 
    {
        setBlockStatus(block, level, true);

        auto & free_list = free_lists[level];

        if (free_list) 
        {
            block->pointers.next = free_list;
            block->pointers.previous = nullptr;
            free_list->pointers.previous = block;
        } else 
        {
            block->pointers.next = nullptr;
            block->pointers.previous = nullptr;
        }

        free_list = block;
    }

    bool isFreeListEmpty(size_t level) const 
    {
        return free_lists[level] == nullptr;
    }

    void removeFromFreeList(MemoryBlock * block, size_t level) 
    {
        assert(!isFreeListEmpty(level));

        setBlockStatus(block, level, false);

        if (block->pointers.next) 
            block->pointers.next->pointers.previous = block->pointers.previous;

        if (block->pointers.previous) 
            block->pointers.previous->pointers.next = block->pointers.next;

        /// Move the end of the list if we delete it
        if (block == free_lists[level]) 
            free_lists[level] = block->pointers.next;

        block->pointers.next = nullptr;
        block->pointers.previous = nullptr;
    }
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

    template <typename TMapped> 
    std::shared_ptr<TMapped> get(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) 
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

    void remove(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */)
    {
        std::lock_guard lock(mutex);
        removeLocked(key);
    }

    /// Similar to the remove function but performs deletions for all keys corresponding to the given type TMapped
    template <typename TMapped>
    void reset(std::lock_guard<std::mutex> & /* cache_lock */) 
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
    void set(const Key & key, const std::shared_ptr<TMapped> & mapped, size_t weight, std::lock_guard<std::mutex> & /* cache_lock */)
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
    static void * alloc(size_t size, size_t alignment = 0)
    {
        checkSize(size);
        // CurrentMemoryTracker::alloc(size);
        return allocNoTrack(size, alignment);
    }

    /// Free memory range.
    static void free(void * buf, size_t size)
    {
        try
        {
            checkSize(size);
            freeNoTrack(buf, size);
            // CurrentMemoryTracker::free(size);
        }
        catch (...)
        {
            DB::tryLogCurrentException("BuddyAllocator::free");
            throw;
        }
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    static void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0)
    {
        checkSize(new_size);

        if (old_size == new_size)
        {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        }
        else
        {
            /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.

            void * new_buf = alloc(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free(buf, old_size);
            buf = new_buf;
        }

        // LOG_INFO(&Poco::Logger::get("UnifiedCache"), "BuddyAllocator: Reallocate buffer {} with old size {} and new size {} (do realloc)", buf, ReadableSize(old_size), ReadableSize(new_size));

        return buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }

    static constexpr bool clear_memory = true;

private:
    static void checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
    }

    static void * allocNoTrack(size_t size, size_t alignment = 0) 
    {
        auto & instance = BuddyArena::instance();
        void * buf = nullptr;
        if (instance.isValid()) 
        {
            buf = instance.malloc(size, alignment);

            if (nullptr == buf)
                DB::throwFromErrno(fmt::format("BuddyAllocator: Cannot allocate memory (BuddyArena) {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            if constexpr (clear_memory)
                memset(buf, 0, size);
        }
        /// TODO: Remove hardcoded constant, only for temporary tests of base Allocator replacement
        // else if (alignment <= MALLOC_MIN_ALIGNMENT)
        else if (alignment <= 8)
        {
            if constexpr (clear_memory)
                buf = ::calloc(size, 1);
            else
                buf = ::malloc(size);

            if (nullptr == buf)
                DB::throwFromErrno(fmt::format("BuddyAllocator: Cannot malloc {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        else
        {
            buf = nullptr;
            int res = posix_memalign(&buf, alignment, size);

            if (0 != res)
                DB::throwFromErrno(fmt::format("BuddyAllocator: Cannot allocate memory (posix_memalign) {}.", ReadableSize(size)),
                    DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);

            if constexpr (clear_memory)
                memset(buf, 0, size);
        }

        return buf;
    }

    static void freeNoTrack(void * buf, size_t size)
    {
        if (nullptr == buf) {
            return;
        }

        auto & instance = BuddyArena::instance();
        if (instance.isValid() && instance.isAllocated(buf)) 
            instance.free(buf, size);
        else
            ::free(buf);
    }
};

}
