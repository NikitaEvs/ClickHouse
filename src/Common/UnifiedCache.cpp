#include <Common/UnifiedCache.h>
#include "Core/Block.h"
#include <Core/Types.h>
#include <base/getPageSize.h>
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <string_view>
#include <sys/mman.h>

namespace DB 
{

BuddyArena::~BuddyArena() noexcept
{
    if (isValid())
        // Do not throw exceptions from invalid munmap
        munmap(arena_buffer, total_size_bytes);
}

bool BuddyArena::isValid() const
{
    return number_of_levels != 0;
}

bool BuddyArena::isAllocated(const void *ptr) const
{
    const auto * arena_end = reinterpret_cast<char *>(arena_buffer) + total_size_bytes;
    return ptr >= arena_buffer && ptr < reinterpret_cast<const void *>(arena_end); 
}

void BuddyArena::initialize(size_t minimal_allocation_size, size_t size, size_t capacity)
{
    /// We will divide the whole size bytes on blocks with size == minimal_allocation_size
    assert(size % minimal_allocation_size == 0);
    assert(capacity % minimal_allocation_size == 0);
    assert(capacity >= size);

    total_size_bytes = capacity;
    number_of_levels = 1;
    minimal_allocation_size_bytes = minimal_allocation_size;

    /// Calculate number of levels
    size_t number_of_blocks_on_level = capacity / minimal_allocation_size; 

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
    // free_min_blocks = min_blocks_num;

    arena_buffer = allocateArena(total_size_bytes);
    const auto * arena_buffer_char_ptr = reinterpret_cast<const char *>(arena_buffer);

    auto * current_storage_ptr = initializeMetaStorage();

    // TODO: Optimize
    /// Initialize free_lists

    /// Calculate number of blocks for the meta storage
    size_t meta_storage_size_minimal_blocks = (current_storage_ptr - arena_buffer_char_ptr) / minimal_allocation_size_bytes;

    /// Calculate the nearest power of 2 that is greater than meta storage size
    size_t meta_storage_size_round_up_to_power_of_2 = 1;
    while (meta_storage_size_round_up_to_power_of_2 < meta_storage_size_minimal_blocks) 
    {
        meta_storage_size_round_up_to_power_of_2 *= 2;
    }

    /// Deallocate (add to the free_lists or shadow_lists) minimal blocks to fill the space between the meta storage and the size that is
    /// the nearest power of 2, so after it we can deallocate blocks with an exponential increasing sizes 
    /// Arena buffer:
    /// [*******-------------|------------------------------------]
    ///  |               |   |
    ///  ^ meta storage  |   ^ meta_storage_size_round_up_to_power_of_2
    ///                  ^ deallocate these blocks on this step
    const auto * minimal_blocks_area_end = arena_buffer_char_ptr + meta_storage_size_round_up_to_power_of_2 * minimal_allocation_size_bytes;
    while (current_storage_ptr != minimal_blocks_area_end) 
    {
        auto * memory_block = reinterpret_cast<MemoryBlock *>(current_storage_ptr);

        if (static_cast<size_t>(current_storage_ptr - arena_buffer_char_ptr) < size) 
        {
            /// Do not fill the size bytes, add the block to the free_lists and reclaim it from the OS
            returnBlockToLists(free_lists, memory_block, number_of_levels - 1);
            reclaimBlock(memory_block, number_of_levels - 1);
        }
        else
        {
            /// Already filled the size bytes, do not add the block to the free_lists
            /// Instead add it to the shadow list, so we can track it
            returnBlockToLists(shadow_lists, memory_block, number_of_levels - 1);
        }

        current_storage_ptr += minimal_allocation_size_bytes;
    }

    /// Deallocate (add to the free_lists or shadow_lists) all next blocks after meta_storage_size_round_up_to_power_of_2 with increasing sizes 
    size_t current_block_size_bytes = meta_storage_size_round_up_to_power_of_2 * minimal_allocation_size;
    size_t current_block_level = calculateLevel(current_block_size_bytes);
    while (current_block_level > 0) 
    {
        auto * memory_block = reinterpret_cast<MemoryBlock *>(current_storage_ptr);

        /// TODO: Add a separate method: code duplication
        if (static_cast<size_t>(current_storage_ptr - arena_buffer_char_ptr) < size) 
        {
            /// Do not fill the size bytes, add the block to the free_lists and reclaim it from the OS
            returnBlockToLists(free_lists, memory_block, current_block_level);
            reclaimBlock(memory_block, current_block_level);
        }
        else
        {
            /// Already filled the size bytes, do not add the block to the free_lists
            /// Instead add it to the shadow list, so we can track it
            returnBlockToLists(shadow_lists, memory_block, current_block_level);
        }

        current_storage_ptr += current_block_size_bytes;
        current_block_size_bytes *= 2;
        --current_block_level;
    }
}

void * BuddyArena::malloc(size_t size, size_t align)
{
    /// TODO: Add assertion
    /// assert that align is a power of 2 and a multiple of sizeof(void*) 
    /// https://en.cppreference.com/w/cpp/memory/c/aligned_alloc

    auto level = calculateLevel(size, align);

    auto * block = allocateBlock(level);
    if (block == nullptr)
        block = acquireBlock(level);
    if (block == nullptr)
        return nullptr;

    setPointerLevel(block, level);

    /// TODO: Remove temp local dummy memory tracker
    const size_t allocated_memory_blocks = 1ull << ((number_of_levels - level) - 1);
    free_min_blocks.fetch_sub(allocated_memory_blocks);
    // if (free_min_blocks % 10000 == 0) {
    //     printMemoryUsageDummy();
    // }

    return block->data;
}

void BuddyArena::free(void * buf) noexcept
{
    auto * block = reinterpret_cast<MemoryBlock *>(buf);
    auto level = getPointerLevel(block);

    deallocateBlock(block, level);

    /// TODO: Remove temp local dummy memory tracker
    const size_t freeing_memory_blocks = 1ull << ((number_of_levels - level) - 1);
    free_min_blocks.fetch_add(freeing_memory_blocks);
    // if (free_min_blocks % 10000 == 0) {
    //     printMemoryUsageDummy();
    // }
}

void BuddyArena::purge()
{
    for (size_t current_level = 0; current_level < number_of_levels; ++current_level) 
    {
        while (auto * block = takeBlockFromLists(free_lists, current_level)) 
            tryReleaseBlock(block, current_level);
    }
}

double BuddyArena::getFreeSpaceRatio() const
{
    /// TODO: Change seq_cst memory_order on the free_min_blocks atomic
    auto occupied_blocks = min_blocks_num - free_min_blocks.load();
    return static_cast<double>(min_blocks_num - occupied_blocks) / min_blocks_num;
}

size_t BuddyArena::getTotalSizeBytes() const
{
    return total_size_bytes;
}

void * BuddyArena::allocateArena(size_t size)
{
    void * buffer = mmap(nullptr, size, PROT_READ | PROT_WRITE, 
                MAP_PRIVATE | MAP_ANONYMOUS, /*fd=*/ -1, /*offset=*/ 0);
    if (MAP_FAILED == buffer)
        DB::throwFromErrno(fmt::format("BuddyArena: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

    return buffer;
}

void BuddyArena::deallocateArena(void * buffer, size_t size) 
{
    // TODO: Not checking the result value
    munmap(buffer, size);
}

size_t BuddyArena::calculateLevel(size_t size) const 
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

size_t BuddyArena::calculateLevel(size_t size, size_t align) const 
{
    if (align < size) 
        /// Will be automatically aligned if align is valid (as it will be power of 2 that less than size)
        return calculateLevel(size);
    else 
        return calculateLevel(align);
}

size_t BuddyArena::calculateBlockSizeOnLevel(size_t level) const 
{
    return minimal_allocation_size_bytes * (1ull << ((number_of_levels - level) - 1));
}

size_t BuddyArena::calculateIndexInLevel(const MemoryBlock * block, size_t level) const 
{
    const auto * block_data = reinterpret_cast<const char *>(block);
    const auto * arena_data = static_cast<const char *>(arena_buffer);
    return (block_data - arena_data) / calculateBlockSizeOnLevel(level);
}

size_t BuddyArena::calculateIndex(const MemoryBlock * block, size_t level) const 
{
    return (1ull << level) + calculateIndexInLevel(block, level) - 1ull;
}

size_t BuddyArena::calculatePointerIndex(const MemoryBlock * block) const 
{
    const auto * block_data = reinterpret_cast<const char *>(block);
    const auto * arena_data = static_cast<const char *>(arena_buffer);
        
    return (block_data - arena_data) / calculateBlockSizeOnLevel(number_of_levels - 1);
}

size_t BuddyArena::calculateMinBlocksNumber(size_t size_bytes) const
{
    return size_bytes / minimal_allocation_size_bytes + (size_bytes % minimal_allocation_size_bytes == 0 ? 0 : 1);
}

BuddyArena::MemoryBlock * BuddyArena::blockFromIndexInLevel(size_t index_in_level, size_t level) const
{
    auto block_size = calculateBlockSizeOnLevel(level); 
    return reinterpret_cast<MemoryBlock *>(static_cast<char *>(arena_buffer) + block_size * index_in_level);
}

size_t BuddyArena::getPointerLevel(const MemoryBlock * block) const 
{
    const auto pointer_index = calculatePointerIndex(block);
    return pointers_levels[pointer_index];
}

void BuddyArena::setPointerLevel(const MemoryBlock * block, size_t level) 
{
    const auto pointer_index = calculatePointerIndex(block);
    pointers_levels[pointer_index] = level;
}

BuddyArena::MemoryBlock * BuddyArena::takeBlockFromLists(GuardedLists & guarded_lists, size_t level)
{
    MemoryBlock * current_block = nullptr;
    size_t current_level = level;
    do 
    {
        std::lock_guard lock(guarded_lists.mutexes[current_level]);

        /// Found a block on this level
        if (!isListEmpty(guarded_lists.lists, current_level)) 
        {
            current_block = guarded_lists.lists[current_level];
            removeFromList(guarded_lists.lists, current_block, current_level);
            setBlockStatus(current_block, current_level, BlockStatus::NotInLists);
            break;
        }

        /// There are no blocks on this level, go to the top
        if (current_level == 0)
            break;
        --current_level;
    } while (current_level != 0);

    /// Already on the top without free blocks
    if (current_block == nullptr) 
        return nullptr;

    /// Iterate from current_level to the level and split current_block 
    while (current_level != level) 
    {
        auto [block, buddy_block] = divideBlock(current_block, current_level);
        {
            std::lock_guard lock(guarded_lists.mutexes[current_level + 1]);
            addToList(guarded_lists.lists, buddy_block, current_level + 1);
            setBlockStatus(buddy_block, current_level + 1, guarded_lists.lists_block_status);
        }
        current_block = block;
        ++current_level;
    }

    return current_block;
}

void BuddyArena::returnBlockToLists(GuardedLists & guarded_lists, MemoryBlock * block, size_t level)
{
    assert(block);

    size_t current_level = level;
    MemoryBlock * current_block = block;
    
    do 
    {
        std::lock_guard lock(guarded_lists.mutexes[current_level]);

        auto * buddy = getBuddy(current_block, current_level);
        if (buddy && (getBlockStatus(buddy, current_level) == guarded_lists.lists_block_status)) 
        {
            // Merge with buddy
            auto * merged_block = mergeWithBuddy(current_block, buddy);
            removeFromList(guarded_lists.lists, buddy, current_level);
            setBlockStatus(buddy, current_level, BlockStatus::NotInLists);
            current_block = merged_block;
        } else 
        {
            addToList(guarded_lists.lists, current_block, current_level);
            setBlockStatus(current_block, current_level, guarded_lists.lists_block_status);
            break;
        }  

        if (current_level == 0)
            break;
        --current_level;
    } while (current_level != 0);
}

BuddyArena::MemoryBlock * BuddyArena::allocateBlock(size_t level) 
{
    return takeBlockFromLists(free_lists, level);
}

void BuddyArena::deallocateBlock(MemoryBlock * block, size_t level) 
{
    returnBlockToLists(free_lists, block, level);
}

bool BuddyArena::adviseBlock(MemoryBlock * block, size_t level) const
{
    const auto block_size = calculateBlockSizeOnLevel(level);

    void * block_ptr = reinterpret_cast<void *>(block->data);
    auto ret = madvise(block_ptr, block_size, MADV_DONTNEED);
    return ret == 0;
}

void BuddyArena::reclaimBlock(MemoryBlock * block, size_t level) const
{
    const auto block_size = calculateBlockSizeOnLevel(level);
    const auto page_num = block_size / kPageSize;

    auto * block_ptr = block->data;

    mlock(reinterpret_cast<void *>(block_ptr), block_size);

    /// Trigger page faults, use volatile to fool the compiler
    for (size_t page_offset = 0; page_offset < page_num; ++page_offset) {
        volatile const auto val = *(block_ptr + page_offset * kPageSize);
        (void)val;
    }
}

BuddyArena::MemoryBlock * BuddyArena::acquireBlock(size_t level)
{
    auto * block = takeBlockFromLists(shadow_lists, level);
    if (block != nullptr) 
        reclaimBlock(block, level);
    return block;
}

void BuddyArena::tryReleaseBlock(MemoryBlock * block, size_t level)
{
    if (adviseBlock(block, level))
        returnBlockToLists(shadow_lists, block, level);
}

char * BuddyArena::initializeMetaStorage() 
{
    /// TODD: Note that if there is a big min_allocation_size, we can potentially waste memory on the unnecessary alignment
    /// Calculate sizes
    size_t block_status_size = (1ull << number_of_levels) - 1ull;
    size_t block_status_size_bytes = sizeof(bool) * block_status_size;
    size_t block_status_size_minimal_blocks = calculateMinBlocksNumber(block_status_size_bytes);

    size_t free_lists_size_bytes = sizeof(MemoryBlock *) * number_of_levels; // NOLINT
    size_t free_lists_size_minimal_blocks = calculateMinBlocksNumber(free_lists_size_bytes);

    size_t shadow_lists_size_bytes = sizeof(MemoryBlock *) * number_of_levels; // NOLINT
    size_t shadow_lists_size_minimal_blocks = calculateMinBlocksNumber(shadow_lists_size_bytes);

    size_t pointers_levels_size = (1ull << (number_of_levels - 1));
    size_t pointers_levels_size_bytes = sizeof(uint8_t) * pointers_levels_size;
    size_t pointers_levels_size_minmal_blocks = calculateMinBlocksNumber(pointers_levels_size_bytes);

    size_t mutexes_size_bytes = sizeof(std::mutex) * number_of_levels;
    size_t mutexes_size_minimal_blocks = calculateMinBlocksNumber(mutexes_size_bytes);
        
    /// Populate pointers 
    auto * current_storage_ptr = reinterpret_cast<char *>(arena_buffer);

    block_status = reinterpret_cast<BlockStatus *>(current_storage_ptr);
    current_storage_ptr += block_status_size_minimal_blocks * minimal_allocation_size_bytes;

    /// Place free_lists.lists and free_lists.mutexes for the better cache locality
    free_lists.lists = reinterpret_cast<MemoryBlock **>(current_storage_ptr);
    current_storage_ptr += free_lists_size_minimal_blocks * minimal_allocation_size_bytes;

    free_lists.mutexes = reinterpret_cast<std::mutex *>(current_storage_ptr);
    current_storage_ptr += mutexes_size_minimal_blocks * minimal_allocation_size_bytes;

    pointers_levels = reinterpret_cast<uint8_t *>(current_storage_ptr);
    current_storage_ptr += pointers_levels_size_minmal_blocks * minimal_allocation_size_bytes;

    /// Place shadow_lists.lists and shadow_lists.mutexes for the better cache locality
    shadow_lists.lists = reinterpret_cast<MemoryBlock **>(current_storage_ptr);
    current_storage_ptr += shadow_lists_size_minimal_blocks * minimal_allocation_size_bytes;

    shadow_lists.mutexes = reinterpret_cast<std::mutex *>(current_storage_ptr);
    current_storage_ptr += mutexes_size_minimal_blocks * minimal_allocation_size_bytes;

    return current_storage_ptr;
}

std::pair<BuddyArena::MemoryBlock *, BuddyArena::MemoryBlock *> BuddyArena::divideBlock(
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

BuddyArena::MemoryBlock * BuddyArena::mergeWithBuddy(MemoryBlock * block, MemoryBlock * buddy)
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

BuddyArena::MemoryBlock * BuddyArena::getBuddy(MemoryBlock * block, size_t level) const 
{
    auto index_in_level = calculateIndexInLevel(block, level);
    size_t buddy_index_in_level = index_in_level % 2 == 0 ? index_in_level + 1 : index_in_level - 1;
    return blockFromIndexInLevel(buddy_index_in_level, level);
}

BuddyArena::BlockStatus BuddyArena::getBlockStatus(const MemoryBlock * block, size_t level) const
{
    auto index_of_block = calculateIndex(block, level);
    return block_status[index_of_block];
}

void BuddyArena::setBlockStatus(const MemoryBlock * block, size_t level, BlockStatus status)
{
    auto index_of_block = calculateIndex(block, level);
    block_status[index_of_block] = status; 
}

void BuddyArena::printMemoryUsageDummy() const 
{
    auto occupied_blocks = min_blocks_num - free_min_blocks;
    double usage = static_cast<double>(occupied_blocks) / min_blocks_num * 100.0;
    std::cout << "***Memory usage: " << occupied_blocks << " / " << min_blocks_num << " (" << std::fixed << std::setprecision(1) << usage << " %)***" << std::endl;
}

void BuddyArena::addToList(MemoryBlock ** lists, MemoryBlock * block, size_t level)
{
    auto & list = lists[level];

    if (list) 
    {
        block->pointers.next = list;
        block->pointers.previous = nullptr;
        list->pointers.previous = block;
    } else 
    {
        block->pointers.next = nullptr;
        block->pointers.previous = nullptr;
    }

    list = block;
}

bool BuddyArena::isListEmpty(MemoryBlock ** lists, size_t level)
{
    return lists[level] == nullptr;
}

void BuddyArena::removeFromList(MemoryBlock ** lists, MemoryBlock * block, size_t level)
{
    assert(!isListEmpty(lists, level));

    if (block->pointers.next) 
        block->pointers.next->pointers.previous = block->pointers.previous;

    if (block->pointers.previous) 
        block->pointers.previous->pointers.next = block->pointers.next;

    /// Move the end of the list if we delete it
    if (block == lists[level]) 
        lists[level] = block->pointers.next;

    block->pointers.next = nullptr;
    block->pointers.previous = nullptr;
}


MemoryBlock::MemoryBlock(size_t size_) : size(size_), is_owner(true)
{
    ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == ptr)
        DB::throwFromErrno(fmt::format("BlockCache: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
}

MemoryBlock::MemoryBlock(void * ptr_, size_t size_) : ptr(ptr_), size(size_), is_owner(false)
{}

MemoryBlock::~MemoryBlock()
{
    if (is_owner && ptr && 0 != munmap(ptr, size))
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
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::allocate(size_t size)
{
    size = roundUp(size, alignment);

    /// Fast path. Look up to size multimap to find free region of specified size.
    {
        std::lock_guard cache_lock(mutex);
        auto it = size_multimap.lower_bound(size, RegionCompareBySize());
        if (size_multimap.end() != it)
            return allocateFromFreeRegion(&*it, size, cache_lock);
    }

    /// Slow path. If nothing was found, ask for rebalancing and check again for the space
    if (rebalance_strategy) 
        rebalance_strategy->shouldRebalance(name, size);

    std::lock_guard cache_lock(mutex);

    if (rebalance_strategy)
    {
        /// Check again for free blocks
        auto rebalanced_it = size_multimap.lower_bound(size, RegionCompareBySize());
        if (size_multimap.end() != rebalanced_it)
            return allocateFromFreeRegion(&*rebalanced_it, size, cache_lock);
    }

    /// Nothing works. Start evictions
    return evict(size, cache_lock);
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
typename BlockCache<Key>::RegionMetadata * BlockCache<Key>::evict(size_t requested_size, std::lock_guard<std::mutex> & cache_lock) noexcept
{
    while (true)
    {
        RegionMetadata * res = evictSome(requested_size, cache_lock);

        /// Nothing to evict. All cache is full and in use - cannot allocate memory.
        if (!res)
            return nullptr;

        /// Not enough. Evict more.
        if (res->size < requested_size)
            continue;

        return allocateFromFreeRegion(res, requested_size, cache_lock);
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


void RebalanceStrategySettings::set(const std::string & key, const Field & field)
{
    rebalance_strategy_settings.emplace(std::make_pair(key, field));
}

void RebalanceStrategySettings::setBlockCacheSetting(
    const std::string & key, 
    const std::string & block_cache_name, 
    const Field & field)
{
    block_caches_settings[block_cache_name].emplace(std::make_pair(key, field));
}

bool RebalanceStrategySettings::tryGet(const std::string & key, Field & field) const 
{
    auto it = rebalance_strategy_settings.find(key);
    if (it == rebalance_strategy_settings.end())
        return false;

    field = (*it).second;
    return true; 
}

bool RebalanceStrategySettings::tryGetBlockCacheSetting(
    const std::string & key, 
    const std::string & block_cache_name, 
    Field & field) const 
{
    auto block_cache_it = block_caches_settings.find(block_cache_name);
    if (block_cache_it == block_caches_settings.end())
        return false;

    const auto & block_cache_settings = block_cache_it->second;
    auto it = block_cache_settings.find(key);
    if (it == block_cache_settings.end())
        return false;

    field = (*it).second;
    return true;
}


template <typename Key>
DummyRebalanceStrategy<Key>::DummyRebalanceStrategy(
    BlockCacheMapping & caches_, const std::vector<std::string> & block_cache_names_)
    : Base(caches_)
    , block_cache_names(block_cache_names_)
{
    for (const auto & cache_name : block_cache_names)
        block_cache_stats.try_emplace(cache_name);
}

template <typename Key>
MemoryBlockList DummyRebalanceStrategy<Key>::initialize(const std::string & /*caller_name*/) 
{
    // nop
    return {};
}

template <typename Key>
void DummyRebalanceStrategy<Key>::shouldRebalance(const std::string &caller_name, size_t new_item_size)
{
    assert(block_cache_stats.count(caller_name) != 0);
    size_t page_size = static_cast<size_t>(::getPageSize());
    size_t required_block_size = std::max(Settings::min_memory_block_size, roundUp(new_item_size, page_size));

    const auto & settings_entry = settings.block_cache_settings.at(caller_name);
    auto & stats_entry = block_cache_stats.at(caller_name);
    if (stats_entry.total_memory_blocks_size + required_block_size <= settings_entry.max_total_size) 
    {
        auto * block = MemoryBlock::create(required_block_size);
        MemoryBlockList memory_blocks; 
        memory_blocks.push_back(*block);
        Base::caches.at(caller_name).addNewMemoryBlocks(memory_blocks);
        stats_entry.total_memory_blocks_size.fetch_add(required_block_size, std::memory_order_relaxed);
    }
}

template <typename Key>
void DummyRebalanceStrategy<Key>::finalize(const std::string &caller_name, MemoryBlockList memory_blocks)
{
    assert(block_cache_stats.count(caller_name) != 0);
    memory_blocks.clear_and_dispose([](MemoryBlock * block) { block->destroy(); });
    block_cache_stats[caller_name].total_memory_blocks_size.store(0, std::memory_order_relaxed);
}

template <typename Key>
void DummyRebalanceStrategy<Key>::initializeWithSettings(const RebalanceStrategySettings & settings_)
{
    Field field;
    for (const auto & cache_name : block_cache_names) {
        size_t current_max_total_size = 0;
        size_t current_max_size_to_evict_on_purging = 0;

        if (settings_.tryGetBlockCacheSetting("max_total_size", cache_name, field))
            current_max_total_size = field.safeGet<size_t>();

        if (settings_.tryGetBlockCacheSetting("max_size_to_evict_on_purging", cache_name, field))
            current_max_size_to_evict_on_purging = field.safeGet<size_t>();
        
        settings.block_cache_settings.try_emplace(cache_name, current_max_total_size, current_max_size_to_evict_on_purging);
    }
}

template <typename Key>
size_t DummyRebalanceStrategy<Key>::getWeight() const
{
    size_t total_weight = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_weight += block_cache.getCacheWeight();
    }
    return total_weight;
}

template <typename Key>
size_t DummyRebalanceStrategy<Key>::getCount() const
{
    size_t total_count = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_count += block_cache.getCacheCount();
    }
    return total_count;
}

template <typename Key>
void DummyRebalanceStrategy<Key>::purge()
{
    shrinkBlockCaches(/*use_limit = */true);
}

template <typename Key>
void DummyRebalanceStrategy<Key>::reset()
{
    shrinkBlockCaches(/*use_limit = */false);
}

template <typename Key>
void DummyRebalanceStrategy<Key>::shrinkBlockCaches(bool use_limit)
{
    for (auto & item : block_cache_stats) 
    {
        const auto & name = item.first;
        auto & cache_stats = item.second;
        const auto & cache_settings = settings.block_cache_settings.at(name);
        const auto eviction_limit = use_limit? cache_settings.max_size_to_evict_on_purging : cache_settings.max_total_size;

        auto blocks = Base::caches.at(name).takeMemoryBlocks(eviction_limit);
        size_t evicted_size = 0;
        blocks.clear_and_dispose([&evicted_size](MemoryBlock * block) 
        { 
            evicted_size += block->size;
            block->destroy(); 
        });
        cache_stats.total_memory_blocks_size.fetch_sub(evicted_size, std::memory_order_relaxed);
    }
}


template <typename Key>
BuddyStaticRebalanceStrategy<Key>::BuddyStaticRebalanceStrategy(
    BlockCacheMapping & caches_, 
    const std::vector<std::string> & block_cache_names_)
    : Base(caches_)
    , block_cache_names(block_cache_names_)
{
    for (const auto & cache_name : block_cache_names)
        block_cache_stats.try_emplace(cache_name);
}

template <typename Key>
MemoryBlockList BuddyStaticRebalanceStrategy<Key>::initialize(const std::string & /*caller_name*/) 
{
    // nop
    return {};
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::shouldRebalance(const std::string & caller_name, size_t new_item_size)
{
    assert(block_cache_stats.count(caller_name) != 0);

    size_t required_block_size = roundUpToPowerOf2(new_item_size, Settings::minimal_allocation_size);
    auto & stats_entry = block_cache_stats[caller_name];

    auto * block_ptr = memory_arena.malloc(required_block_size);
    if (block_ptr == nullptr)
    {
        std::cerr << "Cannot allocate a block with size " << required_block_size << std::endl;
        return;
    }

    auto * block = MemoryBlock::create(block_ptr, required_block_size);

    MemoryBlockList memory_blocks; 
    memory_blocks.push_back(*block);
    Base::caches.at(caller_name).addNewMemoryBlocks(memory_blocks);

    stats_entry.total_memory_blocks_size.fetch_add(required_block_size, std::memory_order_relaxed);
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::finalize(const std::string & caller_name, MemoryBlockList memory_blocks)
{
    assert(block_cache_stats.count(caller_name) != 0);
    memory_blocks.clear_and_dispose([this](MemoryBlock * block) 
    { 
        memory_arena.free(block->ptr);
        block->destroy(); 
    });
    block_cache_stats[caller_name].total_memory_blocks_size.store(0, std::memory_order_relaxed);
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::initializeWithSettings(const RebalanceStrategySettings & settings_)
{
    Field field;
    /// Initialize a rebalance strategy setting
    if (settings_.tryGet("memory_arena_size", field))
        settings.memory_arena_size = field.safeGet<size_t>();

    /// Initialize block cache settings
    for (const auto & cache_name : block_cache_names) {
        size_t current_max_size_to_evict_on_purging = 0;

        if (settings_.tryGetBlockCacheSetting("max_size_to_evict_on_purging", cache_name, field))
            current_max_size_to_evict_on_purging = field.safeGet<size_t>();
        
        settings.block_cache_settings.try_emplace(cache_name, current_max_size_to_evict_on_purging);
    }

    /// Initialize the memory arena
    memory_arena.initialize(Settings::minimal_allocation_size, settings.memory_arena_size, settings.memory_arena_size);
}

template <typename Key>
size_t BuddyStaticRebalanceStrategy<Key>::getWeight() const
{
    size_t total_weight = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_weight += block_cache.getCacheWeight();
    }
    return total_weight;
}

template <typename Key>
size_t BuddyStaticRebalanceStrategy<Key>::getCount() const
{
    size_t total_count = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_count += block_cache.getCacheCount();
    }
    return total_count;
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::purge()
{
    shrinkBlockCaches(/*use_limit = */true);
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::reset()
{
    shrinkBlockCaches(/*use_limit = */false);
}

template <typename Key>
void BuddyStaticRebalanceStrategy<Key>::shrinkBlockCaches(bool use_limit)
{
    for (auto & item : block_cache_stats) 
    {
        const auto & name = item.first;
        auto & cache_stats = item.second;
        const auto & cache_settings = settings.block_cache_settings.at(name);
        const auto eviction_limit = use_limit? cache_settings.max_size_to_evict_on_purging : settings.memory_arena_size;

        auto blocks = Base::caches.at(name).takeMemoryBlocks(eviction_limit);
        size_t evicted_size = 0;
        blocks.clear_and_dispose([this, &evicted_size](MemoryBlock * block) 
        { 
            evicted_size += block->size;
            memory_arena.free(block->ptr);
            block->destroy(); 
        });
        cache_stats.total_memory_blocks_size.fetch_sub(evicted_size, std::memory_order_relaxed);
    }
}


template <typename Key>
BuddyDynamicRebalanceStrategy<Key>::BuddyDynamicRebalanceStrategy(
    BlockCacheMapping & caches_, 
    const std::vector<std::string> & block_cache_names_)
    : Base(caches_)
    , block_cache_names(block_cache_names_)
{
    for (const auto & cache_name : block_cache_names)
        block_cache_stats.try_emplace(cache_name);
}

template <typename Key>
MemoryBlockList BuddyDynamicRebalanceStrategy<Key>::initialize(const std::string & /*caller_name*/) 
{
    // nop
    return {};
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::shouldRebalance(const std::string & caller_name, size_t new_item_size)
{
    assert(block_cache_stats.count(caller_name) != 0);
    size_t required_block_size = roundUpToPowerOf2(new_item_size, Settings::minimal_allocation_size);

    /// Try to steal free memory blocks from other cache pools
    MemoryBlockList stolen_memory_blocks;
    {
        std::lock_guard lock(stealing_mutex);
        for (auto & item : Base::caches) 
        {
            const auto & name = item.first;
            if (name == caller_name) 
                continue;
            auto & block_cache = item.second;

            size_t initial_size = block_cache.getCacheWeight();
            auto blocks = block_cache.takeMemoryBlocks(0);
            stolen_memory_blocks.splice(stolen_memory_blocks.end(), blocks);
            size_t after_steal_size = block_cache.getCacheWeight();
            assert(initial_size >= after_steal_size);

            auto & stats_entry = block_cache_stats[name];
            stats_entry.total_memory_blocks_size.fetch_sub(initial_size - after_steal_size, std::memory_order_relaxed);
        }
    }

    /// Return all stealt blocks to the BuddyArena
    stolen_memory_blocks.clear_and_dispose([this](MemoryBlock * block) 
    { 
        memory_arena.free(block->ptr);
        block->destroy(); 
    });

    /// Take memory from the BuddyArena
    auto * block_ptr = memory_arena.malloc(settings.allocated_memory_multiplier * required_block_size);
    if (block_ptr == nullptr)
        return;

    auto * block = MemoryBlock::create(block_ptr, required_block_size);

    /// Add it to the cache pool
    MemoryBlockList memory_blocks; 
    memory_blocks.push_back(*block);
    Base::caches.at(caller_name).addNewMemoryBlocks(memory_blocks);

    auto & stats_entry = block_cache_stats[caller_name];
    stats_entry.total_memory_blocks_size.fetch_add(required_block_size, std::memory_order_relaxed);
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::finalize(const std::string & caller_name, MemoryBlockList memory_blocks)
{
    assert(block_cache_stats.count(caller_name) != 0);
    memory_blocks.clear_and_dispose([this](MemoryBlock * block) 
    { 
        memory_arena.free(block->ptr);
        block->destroy(); 
    });
    block_cache_stats[caller_name].total_memory_blocks_size.store(0, std::memory_order_relaxed);
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::initializeWithSettings(const RebalanceStrategySettings & settings_)
{
    Field field;
    /// Initialize a rebalance strategy setting
    if (settings_.tryGet("memory_arena_capacity", field))
        settings.memory_arena_capacity = field.safeGet<size_t>();
    
    if (settings_.tryGet("memory_arena_initial_size", field))
        settings.memory_arena_initial_size = field.safeGet<size_t>();

    if (settings_.tryGet("allocated_memory_multiplier", field))
        settings.allocated_memory_multiplier = field.safeGet<size_t>();

    /// Initialize block cache settings
    for (const auto & cache_name : block_cache_names) {
        size_t current_max_size_to_evict_on_purging = 0;

        if (settings_.tryGetBlockCacheSetting("max_size_to_evict_on_purging", cache_name, field))
            current_max_size_to_evict_on_purging = field.safeGet<size_t>();
        
        settings.block_cache_settings.try_emplace(cache_name, current_max_size_to_evict_on_purging);
    }

    /// Initialize the memory arena
    memory_arena.initialize(Settings::minimal_allocation_size, settings.memory_arena_initial_size, settings.memory_arena_capacity);
}

template <typename Key>
size_t BuddyDynamicRebalanceStrategy<Key>::getWeight() const
{
    size_t total_weight = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_weight += block_cache.getCacheWeight();
    }
    return total_weight;
}

template <typename Key>
size_t BuddyDynamicRebalanceStrategy<Key>::getCount() const
{
    size_t total_count = 0;
    for (const auto & item : Base::caches) 
    {
        const auto & block_cache = item.second;
        total_count += block_cache.getCacheCount();
    }
    return total_count;
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::purge()
{
    shrinkBlockCaches(/*use_limit = */true);
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::reset()
{
    shrinkBlockCaches(/*use_limit = */false);
}

template <typename Key>
void BuddyDynamicRebalanceStrategy<Key>::shrinkBlockCaches(bool use_limit)
{
    for (auto & item : block_cache_stats) 
    {
        const auto & name = item.first;
        auto & cache_stats = item.second;
        const auto & cache_settings = settings.block_cache_settings.at(name);
        const auto eviction_limit = use_limit? cache_settings.max_size_to_evict_on_purging : settings.memory_arena_capacity;

        auto blocks = Base::caches.at(name).takeMemoryBlocks(eviction_limit);
        size_t evicted_size = 0;
        blocks.clear_and_dispose([this, &evicted_size](MemoryBlock * block) 
        { 
            evicted_size += block->size;
            memory_arena.free(block->ptr);
            block->destroy(); 
        });
        cache_stats.total_memory_blocks_size.fetch_sub(evicted_size, std::memory_order_relaxed);
    }
}


template <typename Key>
void BlockCachesManager<Key>::initialize(
    const std::vector<std::string> & block_cache_names,
    std::string_view rebalance_strategy_name,
    const RebalanceStrategySettings & rebalance_strategy_settings)
{
    std::cerr << "Rebalance strategy name: " << rebalance_strategy_name << std::endl;
    if (rebalance_strategy_name.empty()) 
        rebalance_strategy_name = default_rebalance_strategy;

    if (rebalance_strategy_name == "dummy")
        rebalance_strategy = std::make_shared<DummyRebalanceStrategy<Key>>(block_caches, block_cache_names);
    else if (rebalance_strategy_name == "buddy_static")
        rebalance_strategy = std::make_shared<BuddyStaticRebalanceStrategy<Key>>(block_caches, block_cache_names);
    else if (rebalance_strategy_name == "buddy_dynamic")
        rebalance_strategy = std::make_shared<BuddyDynamicRebalanceStrategy<Key>>(block_caches, block_cache_names);
    else
        throw Exception("Invalid rebalance strategy name", ErrorCodes::BAD_ARGUMENTS);

    rebalance_strategy->initializeWithSettings(rebalance_strategy_settings);

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
