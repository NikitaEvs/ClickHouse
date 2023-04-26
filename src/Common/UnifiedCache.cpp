#include <Common/UnifiedCache.h>
#include <Core/Types.h>
#include <base/getPageSize.h>
#include <atomic>
#include <cstdlib>
#include <sys/mman.h>

namespace DB 
{

template <typename Key>
BlockCache<Key>::Chunk::Chunk(size_t size_) : size(size_)
{
    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == ptr)
        DB::throwFromErrno(fmt::format("BlockCache: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
}

template <typename Key>
BlockCache<Key>::Chunk::~Chunk()
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
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::addNewChunk(Chunk * chunk)
{
    total_chunks_size += chunk->size;

    auto * free_region = RegionMetadata::create();
    free_region->ptr = chunk->ptr;
    assert(free_region->ptr != nullptr);
    free_region->chunk = chunk;
    free_region->size = chunk->size;

    adjacency_list.push_back(*free_region);
    size_multimap.insert(*free_region);
    free_chunks.push_back(*chunk);

    return free_region;
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocateNewChunk(size_t size)
{
    auto * chunk = Chunk::create(size);
    return addNewChunk(chunk);
}

/// Precondition: free_region.size >= size.
template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocateFromFreeRegion(typename BlockCache<Key>::RegionMetadata * free_region, size_t size)
{
    if (!free_region)
        return nullptr;

    if (free_region->size == size)
    {
        // Move chunk from free list to the acquired list if the region was the only one in the chunk
        if (free_region->isChunkOwner()) {
            free_chunks.erase(free_chunks.iterator_to(*free_region->chunk));
            acquired_chunks.push_back(*free_region->chunk);
        }
        size_multimap.erase(size_multimap.iterator_to(*free_region));
        return free_region;
    }

    auto * allocated_region = RegionMetadata::create();
    allocated_region->ptr = free_region->ptr;
    assert(allocated_region->ptr != nullptr);
    allocated_region->chunk = free_region->chunk;
    allocated_region->size = size;

    // Move chunk from free list to the acquired list if the region was the only one in the chunk
    if (free_region->isChunkOwner()) {
        free_chunks.erase(free_chunks.iterator_to(*free_region->chunk));
        acquired_chunks.push_back(*free_region->chunk);
    }
    size_multimap.erase(size_multimap.iterator_to(*free_region));
    free_region->size -= size;
    free_region->char_ptr += size;
    // Do not add corresponding chunk in the acquired_chunks as we've alredy did it for the free_region
    size_multimap.insert(*free_region);

    adjacency_list.insert(adjacency_list.iterator_to(*free_region), *allocated_region);
    return allocated_region;
}

static size_t roundUp(size_t x, size_t rounding)
{
    return (x + (rounding - 1)) / rounding * rounding;
}

template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocate(size_t size)
{
    size = roundUp(size, alignment);

    /// Look up to size multimap to find free region of specified size.
    auto it = size_multimap.lower_bound(size, RegionCompareBySize());
    if (size_multimap.end() != it)
        return allocateFromFreeRegion(&*it, size);

    /// If nothing was found and total size of allocated chunks plus required size is lower than maximum,
    ///  allocate a new chunk.
    size_t page_size = static_cast<size_t>(::getPageSize());
    size_t required_chunk_size = std::max(min_chunk_size, roundUp(size, page_size));
    if (total_chunks_size + required_chunk_size <= max_total_size)
    {
        /// Create free region spanning through chunk.
        RegionMetadata * free_region = allocateNewChunk(required_chunk_size);
        return allocateFromFreeRegion(free_region, size);
    }

    while (true)
    {
        RegionMetadata * res = evictSome(size);

        /// Nothing to evict. All cache is full and in use - cannot allocate memory.
        if (!res)
            return nullptr;

        /// Not enough. Evict more.
        if (res->size < size)
            continue;

        return allocateFromFreeRegion(res, size);
    }
}

/// Precondition: region is not in lru_list, not in key_map, not in size_multimap.
/// Postcondition: region is not in lru_list, not in key_map,
///  inserted into size_multimap, possibly coalesced with adjacent free regions.
template <typename Key>
void BlockCache<Key>::freeRegion(typename BlockCache<Key>::RegionMetadata * region) noexcept
{
    if (!region)
        return;

    auto adjacency_list_it = adjacency_list.iterator_to(*region);

    auto left_it = adjacency_list_it;
    if (left_it != adjacency_list.begin())
    {
        --left_it;

        if (left_it->chunk == region->chunk && left_it->isFree())
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
        if (right_it->chunk == region->chunk && right_it->isFree())
        {
            region->size += right_it->size;
            size_multimap.erase(size_multimap.iterator_to(*right_it));
            adjacency_list.erase_and_dispose(right_it, [](RegionMetadata * elem) { elem->destroy(); });
        }
    }

    size_multimap.insert(*region);
    if (region->isChunkOwner()) {
        acquired_chunks.erase(acquired_chunks.iterator_to(*region->chunk));
        free_chunks.push_back(*region->chunk);
    }
}

template <typename Key>
void BlockCache<Key>::evictRegion(RegionMetadata * evicted_region) noexcept
{
    lru_list.erase(lru_list.iterator_to(*evicted_region));

    if (evicted_region->KeyMapHook::is_linked())
        key_map.erase(key_map.iterator_to(*evicted_region));

    freeRegion(evicted_region);
}

/// Evict region from cache and return it, coalesced with nearby free regions.
/// While size is not enough, evict adjacent regions at right, if any.
/// If nothing to evict, returns nullptr.
/// Region is removed from lru_list and key_map and inserted into size_multimap.
template <typename Key>
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::evictSome(size_t requested_size) noexcept
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

        evictRegion(&evicted_region);

        std::cerr << "Total useful cache size: " << total_useful_cache_size.load() << " evicted region size " << evicted_region.size << std::endl;

        if (evicted_region.size >= requested_size)
            return &evicted_region;

        ++it;
        if (it == adjacency_list.end() || it->chunk != evicted_region.chunk || !it->LRUListHook::is_linked())
            return &evicted_region;
    }
}

template <typename Key>
void BlockCache<Key>::shrink(size_t max_size_to_evict)
{
    auto chunks = takeChunks(max_size_to_evict);
    chunks.clear_and_dispose([](Chunk * chunk) { chunk->destroy(); });
}

template <typename Key>
BlockCache<Key>::~BlockCache()
{
    key_map.clear();
    size_multimap.clear();
    lru_list.clear();
    adjacency_list.clear_and_dispose([](RegionMetadata * elem) { elem->destroy(); });

    acquired_chunks.clear_and_dispose([](Chunk * chunk) { chunk->destroy(); });
    free_chunks.clear_and_dispose([](Chunk * chunk) { chunk->destroy(); });
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
void BlockCache<Key>::reset()
{
    shrink(total_chunks_size);
}

template <typename Key>
void BlockCache<Key>::purge()
{
    shrink(max_size_to_evict_on_purging);
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
typename BlockCache<Key>::ChunkStorageList BlockCache<Key>::takeChunks(size_t max_size_to_evict)
{
    std::lock_guard lock(mutex);
    
    /// Firstly, try to evict entries
    /// If max_size_to_evict == 0, take only free chunks in the moment
    size_t evicted_size = 0;
    while (evicted_size < max_size_to_evict) 
    {
        std::cerr << "Already evicted: " << evicted_size << " , to evict: " << max_size_to_evict - evicted_size;
        const auto * evicted_region = evictSome(max_size_to_evict - evicted_size);
        if (evicted_region == nullptr)
            break;
        evicted_size += evicted_region->size;
    }

    /// Secondly, take all free chunks
    ChunkStorageList list; 
    list.swap(free_chunks);
    
    /// Thirdly, remove all free regions from sizemap as we will take chunks under them
    /// TODO: Optimize full scan of sizemap here, we can do linear scan before eviction (as normally we wouldn't have a lot of free regions) 
    /// and then add some flag to the evictSome function says that we don't need to add to the sizemap free regions which cover whole chunks

    /// All regions with smaller sizes won't cover the whole chunk
    auto it = size_multimap.lower_bound(min_chunk_size, RegionCompareBySize());
    while (it != size_multimap.end()) 
    {
        if (it->isChunkOwner()) 
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
    for (const auto & chunk : list) {
        total_chunks_size -= chunk.size;
    }

    return list;
}

template <typename Key>
void BlockCache<Key>::addNewChunks(typename BlockCache<Key>::ChunkStorageList & chunk_list)
{
    std::lock_guard lock(mutex);

    /// Populate 
    for (auto & chunk : chunk_list) 
        addNewChunk(&chunk);
}


template class BlockCache<UInt128>;

}
