#include <Common/UnifiedCache.h>
#include "Access/ContextAccess.h"
#include <Core/Types.h>
#include <base/getPageSize.h>
#include <atomic>
#include <cstdlib>
#include <string_view>
#include <sys/mman.h>

namespace DB 
{

MemoryBlock::MemoryBlock(size_t size_) : size(size_)
{
    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == ptr)
        DB::throwFromErrno(fmt::format("BlockCache: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
}

MemoryBlock::~MemoryBlock()
{
    if (ptr && 0 != munmap(ptr, size))
        DB::throwFromErrno(fmt::format("BlockCache: Cannot munmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_MUNMAP);
}


template <typename Key>
BlockCache<Key>::Holder::Holder(BlockCache<Key> & cache_, BlockCache<Key>::RegionMetadata & region_, std::lock_guard<std::mutex>&)
    : cache(cache_)
    , region(region_)
    , region_ptr(&region_)
{
    /// Remove this region from the lru_list, so we can't evict it
    if (++region.refcount == 1 && region.LRUListHook::is_linked())
        cache.lru_list.erase(cache.lru_list.iterator_to(region));
}

template <typename Key>
BlockCache<Key>::Holder::~Holder()
{
    /// Add this region to the lru_list if there are no references 
    std::lock_guard cache_lock(cache.mutex);
    if (--region.refcount == 0)
        cache.lru_list.push_back(region);
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::addNewRegion(
    MemoryBlock & memory_block, 
    std::lock_guard<std::mutex> & /*cache_lock*/)
{
    auto * free_region = RegionMetadata::create();
    free_region->ptr = memory_block.ptr;
    assert(free_region->ptr != nullptr);
    free_region->memory_block = &memory_block;
    free_region->size = memory_block.size;

    adjacency_list.push_back(*free_region);
    size_multimap.insert(*free_region);

    return free_region;
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocateFromFreeRegion(
    typename BlockCache<Key>::RegionMetadata * free_region, 
    size_t size,
    std::lock_guard<std::mutex> & /*cache_lock*/)
{
    if (!free_region)
        return nullptr;

    if (free_region->size == size)
    {
        // Move memory blocks from free list to the acquired list if the region was the only one in the block 
        if (free_region->isBlockOwner()) 
        {
            free_memory_blocks.erase(free_memory_blocks.iterator_to(*free_region->memory_block));
            acquired_memory_blocks.push_back(*free_region->memory_block);
        }
        size_multimap.erase(size_multimap.iterator_to(*free_region));
        return free_region;
    }

    auto * allocated_region = RegionMetadata::create();
    allocated_region->ptr = free_region->ptr;
    assert(allocated_region->ptr != nullptr);
    allocated_region->memory_block = free_region->memory_block;
    allocated_region->size = size;

    // Move block from free list to the acquired list if the region was the only one in the block 
    if (free_region->isBlockOwner()) 
    {
        free_memory_blocks.erase(free_memory_blocks.iterator_to(*free_region->memory_block));
        acquired_memory_blocks.push_back(*free_region->memory_block);
    }
    size_multimap.erase(size_multimap.iterator_to(*free_region));
    free_region->size -= size;
    free_region->char_ptr += size;
    // Do not add corresponding block in the acquired_blocks as we've alredy did it for the free_region
    size_multimap.insert(*free_region);

    adjacency_list.insert(adjacency_list.iterator_to(*free_region), *allocated_region);
    return allocated_region;
}

static size_t roundUp(size_t x, size_t rounding)
{
    return (x + (rounding - 1)) / rounding * rounding;
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocate(size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    size = roundUp(size, alignment);

    /// Fast path. Look up to size multimap to find free region of specified size.
    auto it = size_multimap.lower_bound(size, RegionCompareBySize());
    if (size_multimap.end() != it)
        return allocateFromFreeRegion(&*it, size, cache_lock);

    /// If nothing was found, ask for rebalancing and check again for the space
    if (rebalance_strategy) 
    {
        rebalance_strategy->shouldRebalance(name, size, cache_lock);
        auto rebalanced_it = size_multimap.lower_bound(size, RegionCompareBySize());
        if (size_multimap.end() != rebalanced_it)
            return allocateFromFreeRegion(&*rebalanced_it, size, cache_lock);
    }

    /// Start evictions
    while (true)
    {
        RegionMetadata * res = evictSome(size, cache_lock);

        /// Nothing to evict. All cache is full and in use - cannot allocate memory.
        if (!res)
            return nullptr;

        /// Not enough. Evict more.
        if (res->size < size)
            continue;

        return allocateFromFreeRegion(res, size, cache_lock);
    }
}

template <typename Key>
void BlockCache<Key>::freeRegion(typename BlockCache<Key>::RegionMetadata * region, std::lock_guard<std::mutex> & /*cache_lock*/) noexcept
{
    if (!region)
        return;

    auto adjacency_list_it = adjacency_list.iterator_to(*region);

    auto left_it = adjacency_list_it;
    if (left_it != adjacency_list.begin())
    {
        --left_it;

        if (left_it->memory_block == region->memory_block && left_it->isFree())
        {
            region->size += left_it->size;
            region->char_ptr -= left_it->size; 
            size_multimap.erase(size_multimap.iterator_to(*left_it));
            adjacency_list.erase_and_dispose(left_it, [](RegionMetadata * elem) { elem->destroy(); });
        }
    }

    auto right_it = adjacency_list_it;
    ++right_it;
    if (right_it != adjacency_list.end())
    {
        if (right_it->memory_block == region->memory_block && right_it->isFree())
        {
            region->size += right_it->size;
            size_multimap.erase(size_multimap.iterator_to(*right_it));
            adjacency_list.erase_and_dispose(right_it, [](RegionMetadata * elem) { elem->destroy(); });
        }
    }

    size_multimap.insert(*region);
    if (region->isBlockOwner()) {
        acquired_memory_blocks.erase(acquired_memory_blocks.iterator_to(*region->memory_block));
        acquired_memory_blocks.push_back(*region->memory_block);
    }
}

template <typename Key>
void BlockCache<Key>::evictRegion(RegionMetadata * evicted_region, std::lock_guard<std::mutex> & cache_lock) noexcept
{
    lru_list.erase(lru_list.iterator_to(*evicted_region));

    if (evicted_region->KeyMapHook::is_linked())
        key_map.erase(key_map.iterator_to(*evicted_region));

    freeRegion(evicted_region, cache_lock);
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::evictSome(size_t requested_size, std::lock_guard<std::mutex> & cache_lock) noexcept
{
    if (lru_list.empty()) 
        return nullptr;

    auto it = adjacency_list.iterator_to(lru_list.front());

    while (true)
    {
        RegionMetadata & evicted_region = *it;

        /// Statistics
        /// Note: change total_useful_size before eviction of the region as we could coalesce with neighbors regions (which can be initially free)
        total_useful_cache_size.fetch_sub(evicted_region.size, std::memory_order_relaxed);
        total_useful_cache_count.fetch_sub(1, std::memory_order_relaxed);

        evictRegion(&evicted_region, cache_lock);

        if (evicted_region.size >= requested_size)
            return &evicted_region;

        ++it;
        if (it == adjacency_list.end() || 
            it->memory_block != evicted_region.memory_block || 
            !it->LRUListHook::is_linked())
            return &evicted_region;
    }
}

template <typename Key>
BlockCache<Key>::BlockCache(const std::string & name_, std::shared_ptr<RebalanceStrategy> rebalance_strategy_)
    : name(name_), rebalance_strategy(std::move(rebalance_strategy_))
{
    auto initial_memory_blocks = rebalance_strategy->initialize(name);
    addNewMemoryBlocks(initial_memory_blocks);
}

template <typename Key>
BlockCache<Key>::~BlockCache()
{
    key_map.clear();
    size_multimap.clear();
    lru_list.clear();
    adjacency_list.clear_and_dispose([](RegionMetadata * elem) { elem->destroy(); });

    auto remaining_blocks = std::move(acquired_memory_blocks);
    remaining_blocks.splice(remaining_blocks.end(), free_memory_blocks);

    rebalance_strategy->finalize(name, std::move(remaining_blocks));

    acquired_memory_blocks.clear();
    free_memory_blocks.clear();
}

template <typename Key>
typename BlockCache<Key>::HolderPtr BlockCache<Key>::get(const Key & key)
{
    std::lock_guard cache_lock(mutex);    

    auto it = key_map.find(key, RegionCompareByKey());
    if (key_map.end() != it)
    {
        return new Holder(*this, *it, cache_lock);
    }
    return nullptr;
}

template <typename Key>
size_t BlockCache<Key>::getCacheWeight() const
{
    return total_useful_cache_size.load(std::memory_order_relaxed);
}

template <typename Key>
size_t BlockCache<Key>::getCacheCount() const
{
    return total_useful_cache_count.load(std::memory_order_relaxed);
}

template <typename Key>
MemoryBlockList BlockCache<Key>::takeMemoryBlocks(size_t max_size_to_evict, std::lock_guard<std::mutex> & cache_lock)
{
    /// Firstly, try to evict entries
    /// If max_size_to_evict == 0, take only free blocks in the moment
    size_t evicted_size = 0;
    while (evicted_size < max_size_to_evict) 
    {
        const auto * evicted_region = evictSome(max_size_to_evict - evicted_size, cache_lock);
        if (evicted_region == nullptr)
            break;
        evicted_size += evicted_region->size;
    }

    /// Secondly, take all free blocks 
    MemoryBlockList list; 
    list.swap(free_memory_blocks);
    
    /// Thirdly, remove all free regions from sizemap as we will take blocks under them
    /// TODO: Optimize full scan of sizemap here, we can do linear scan before eviction (as normally we wouldn't have a lot of free regions) 
    /// and then add some flag to the evictSome function says that we don't need to add to the sizemap free regions which cover whole block 

    /// All regions with smaller sizes won't cover the whole block 
    auto it = size_multimap.lower_bound(min_block_size, RegionCompareBySize());
    while (it != size_multimap.end()) 
    {
        if (it->isBlockOwner()) 
        {
            auto previous_it = it;
            ++it;
            size_multimap.erase(previous_it);
        } else 
        {
            ++it;
        }
    }

    /// TODO: Remove it after moving to the rebalance strategy
    for (const auto & block : list) {
        total_memory_blocks_size -= block.size;
    }

    return list;
}

template <typename Key>
MemoryBlockList BlockCache<Key>::takeMemoryBlocks(size_t max_size_to_evict)
{
    std::lock_guard lock(mutex);
    
    return takeMemoryBlocks(max_size_to_evict, lock);
}

template <typename Key>
void BlockCache<Key>::addNewMemoryBlocks(MemoryBlockList & memory_blocks, std::lock_guard<std::mutex> & cache_lock)
{
    /// Populate regions and update the total size
    for (auto & block : memory_blocks) 
    {
        addNewRegion(block, cache_lock);
        total_memory_blocks_size += block.size;
    }

    /// Add memory_blocks to the free_memory_blocks list
    free_memory_blocks.splice(free_memory_blocks.end(), memory_blocks);
}

template <typename Key>
void BlockCache<Key>::addNewMemoryBlocks(MemoryBlockList & memory_blocks)
{
    std::lock_guard lock(mutex);

    addNewMemoryBlocks(memory_blocks, lock);
}


template <typename Key>
DummyRebalanceStrategy<Key>::DummyRebalanceStrategy(
    BlockCacheMapping & caches_, 
    const BlockCacheSettingsMapping & cache_settings)
    : Base(caches_)
{
    for (const auto & item : cache_settings)
    {
        const auto & cache_name = item.first;
        const auto & cache_setting = item.second;
        block_cache_stats.try_emplace(cache_name, cache_setting);
    }
}

template <typename Key>
MemoryBlockList DummyRebalanceStrategy<Key>::initialize(const std::string & /*caller_name*/) 
{
    // nop
    return {};
}

template <typename Key>
void DummyRebalanceStrategy<Key>::shouldRebalance(const std::string &caller_name, size_t new_item_size, std::lock_guard<std::mutex> &cache_lock)
{
    assert(block_cache_stats.count(caller_name) != 0);
    size_t page_size = static_cast<size_t>(::getPageSize());
    size_t required_block_size = std::max(min_memory_block_size, roundUp(new_item_size, page_size));
    auto & stats_entry = block_cache_stats[caller_name];
    if (stats_entry.total_memory_blocks_size + required_block_size <= stats_entry.max_total_size) 
    {
        auto * block = MemoryBlock::create(required_block_size);
        MemoryBlockList memory_blocks; 
        memory_blocks.push_back(*block);
        Base::caches.at(caller_name).addNewMemoryBlocks(memory_blocks, cache_lock);
        stats_entry.total_memory_blocks_size += required_block_size;
    }
}

template <typename Key>
void DummyRebalanceStrategy<Key>::finalize(const std::string &caller_name, MemoryBlockList memory_blocks)
{
    assert(block_cache_stats.count(caller_name) != 0);
    memory_blocks.clear_and_dispose([](MemoryBlock * block) { block->destroy(); });
    block_cache_stats[caller_name].total_memory_blocks_size = 0;
}

template <typename Key>
size_t DummyRebalanceStrategy<Key>::getWeight() const
{
    size_t total_weight = 0;
    for (const auto & item : Base::caches) {
        const auto & block_cache = item.second;
        total_weight += block_cache.getCacheWeight();
    }
    return total_weight;
}

template <typename Key>
size_t DummyRebalanceStrategy<Key>::getCount() const
{
    size_t total_count = 0;
    for (const auto & item : Base::caches) {
        const auto & block_cache = item.second;
        total_count += block_cache.getCacheCount();
    }
    return total_count;
}

template <typename Key>
void DummyRebalanceStrategy<Key>::purge()
{
    for (const auto & item : block_cache_stats) 
    {
        const auto & name = item.first;
        const auto & cache_stats = item.second;
        if (cache_stats.max_size_to_evict_on_purging > 0)
        {
            auto blocks = Base::caches.at(name).takeMemoryBlocks(cache_stats.max_size_to_evict_on_purging);
            size_t evicted_size = 0;
            blocks.clear_and_dispose([&evicted_size](MemoryBlock * block) 
            { 
                evicted_size += block->size;
                block->destroy(); 
            });
            block_cache_stats.at(name).total_memory_blocks_size -= evicted_size;
        }
    }
}

template <typename Key>
void DummyRebalanceStrategy<Key>::reset()
{
    for (const auto & item : block_cache_stats) 
    {
        const auto & name = item.first;
        const auto & cache_stats = item.second;
        auto blocks = Base::caches.at(name).takeMemoryBlocks(cache_stats.max_total_size);
        blocks.clear_and_dispose([](MemoryBlock * block) { block->destroy(); });
    }
}


template <typename Key>
BuddyRebalanceStrategy<Key>::BuddyRebalanceStrategy(
    BlockCacheMapping & caches_, 
    const BlockCacheSettingsMapping & cache_settings)
    : Base(caches_)
{
    for (const auto & item : cache_settings)
    {
        const auto & cache_name = item.first;
        const auto & cache_setting = item.second;
        block_cache_stats.try_emplace(cache_name, cache_setting);
    }
}

template <typename Key>
MemoryBlockList BuddyRebalanceStrategy<Key>::initialize(const std::string & /*caller_name*/) 
{
    // nop
    return {};
}

template <typename Key>
void BuddyRebalanceStrategy<Key>::shouldRebalance(const std::string &/*caller_name*/, size_t /*new_item_size*/, std::lock_guard<std::mutex> &/*cache_lock*/)
{
}

template <typename Key>
void BuddyRebalanceStrategy<Key>::finalize(const std::string &/*caller_name*/, MemoryBlockList /*memory_blocks*/)
{

}

template <typename Key>
size_t BuddyRebalanceStrategy<Key>::getWeight() const
{
    size_t total_weight = 0;
    for (const auto & item : Base::caches) {
        const auto & block_cache = item.second;
        total_weight += block_cache.getCacheWeight();
    }
    return total_weight;
}

template <typename Key>
size_t BuddyRebalanceStrategy<Key>::getCount() const
{
    size_t total_count = 0;
    for (const auto & item : Base::caches) {
        const auto & block_cache = item.second;
        total_count += block_cache.getCacheCount();
    }
    return total_count;
}

template <typename Key>
void BuddyRebalanceStrategy<Key>::purge()
{

}

template <typename Key>
void BuddyRebalanceStrategy<Key>::reset()
{

}


template <typename Key>
void BlockCachesManager<Key>::initialize(
    const std::vector<std::string> & block_cache_names,
    std::string_view rebalance_strategy_name,
    const BlockCacheSettingsMapping & cache_settings)
{
    if (rebalance_strategy_name.empty()) {
        rebalance_strategy_name = default_rebalance_strategy;
    } 
    if (rebalance_strategy_name == "dummy") {
        rebalance_strategy = std::make_shared<DummyRebalanceStrategy<Key>>(block_caches, cache_settings);
    } else {
        throw Exception("Invalid rebalance strategy name", ErrorCodes::BAD_ARGUMENTS);
    }

    /// Populate block_caches mapping, the key is block_cache_name and block_cache_name with rebalance_strategy are provided in the constructor 
    for (const auto & block_cache_name : block_cache_names)
        block_caches.try_emplace(block_cache_name, block_cache_name, rebalance_strategy);
}

template <typename Key>
BlockCache<Key> & BlockCachesManager<Key>::getBlockCacheInstance(const std::string & name) 
{
    return block_caches.at(name);
}

template <typename Key>
size_t BlockCachesManager<Key>::getCacheWeight() const
{
    assert(rebalance_strategy != nullptr);
    return rebalance_strategy->getWeight();
} 

template <typename Key>
size_t BlockCachesManager<Key>::getCacheCount() const
{
    assert(rebalance_strategy != nullptr);
    return rebalance_strategy->getCount();
}

template <typename Key>
void BlockCachesManager<Key>::purge()
{
    rebalance_strategy->purge();
}

template <typename Key>
void BlockCachesManager<Key>::reset()
{
    rebalance_strategy->reset();
}


template class BlockCache<UInt128>;
template class DummyRebalanceStrategy<UInt128>;
template class BlockCachesManager<UInt128>;

}
