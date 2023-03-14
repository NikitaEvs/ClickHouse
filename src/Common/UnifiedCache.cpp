#include "UnifiedCache.h"
#include "Common/HashTable/Hash.h"

namespace DB {

extern const size_t UNIFIED_ALLOC_THRESHOLD = static_cast<size_t>(10) * 1024 * 1024 * 1024;

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

void BuddyArena::initialize(size_t minimal_allocation_size, size_t size)
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

void * BuddyArena::malloc(size_t size, size_t align)
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

void BuddyArena::free(void * buf) noexcept
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
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, /*fd=*/ -1, /*offset=*/ 0);
    if (MAP_FAILED == buffer)
        DB::throwFromErrno(fmt::format("BuddyArena: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

    /// TODO: Remove temporary hack to make the memory tracker works
    /// As we allocated the memory arena, we don't want to take it into account in the 
    /// memory tracker
    // CurrentMemoryTracker::free(size);

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

BuddyArena::MemoryBlock * BuddyArena::allocateBlock(size_t level) 
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

void BuddyArena::deallocateBlock(MemoryBlock * block, size_t level) 
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

char * BuddyArena::initializeMetaStorage() 
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

bool BuddyArena::getBlockStatus(const MemoryBlock * block, size_t level) const
{
    auto index_of_block = calculateIndex(block, level);
    return block_status[index_of_block];
}

void BuddyArena::setBlockStatus(const MemoryBlock * block, size_t level, bool status)
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

void BuddyArena::addToFreeList(MemoryBlock * block, size_t level) 
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

bool BuddyArena::isFreeListEmpty(size_t level) const 
{
    return free_lists[level] == nullptr;
}

void BuddyArena::removeFromFreeList(MemoryBlock * block, size_t level) 
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

void * BuddyAllocator::alloc(size_t size, size_t alignment)
{
    checkSize(size);
    CurrentMemoryTracker::allocNoThrow(size);
    return allocNoTrack(size, alignment);
}

void BuddyAllocator::free(void * buf, size_t size)
{
    try
    {
        checkSize(size);
        freeNoTrack(buf, size);
        CurrentMemoryTracker::free(size);
    }
    catch (...)
    {
        DB::tryLogCurrentException("BuddyAllocator::free");
        throw;
    }
}

void * BuddyAllocator::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    checkSize(new_size);

    if (old_size == new_size)
    {
        /// nothing to do.
        /// BTW, it's not possible to change alignment while doing realloc.
    }
    /// TODO: Remove hardcoded constant, only for temporary tests of base Allocator replacement
    // else if (alignment <= MALLOC_MIN_ALIGNMENT)
    else if (old_size < DB::UNIFIED_ALLOC_THRESHOLD && new_size < DB::UNIFIED_ALLOC_THRESHOLD 
             && alignment <= 8)
    {
        /// Resize malloc'd memory region with no special alignment requirement.
        CurrentMemoryTracker::realloc(old_size, new_size);

        void * new_buf = ::realloc(buf, new_size);
        if (nullptr == new_buf)
            DB::throwFromErrno(fmt::format("BuddyAllocator: Cannot realloc from {} to {}.", ReadableSize(old_size), ReadableSize(new_size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        buf = new_buf;
        if constexpr (clear_memory)
            if (new_size > old_size)
                memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
    }
    else
    {
        /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.

        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        free(buf, old_size);
        buf = new_buf;
    }

    return buf;
}

void BuddyAllocator::checkSize(size_t size)
{
    /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
    if (size >= 0x8000000000000000ULL)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

void * BuddyAllocator::allocNoTrack(size_t size, size_t alignment) 
{
    auto & instance = BuddyArena::instance();
    void * buf = nullptr;

    if (size >= DB::UNIFIED_ALLOC_THRESHOLD && instance.isValid())
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

void BuddyAllocator::freeNoTrack(void * buf, size_t /*size*/)
{
    if (nullptr == buf) {
        return;
    }

    auto & instance = BuddyArena::instance();
    if (instance.isValid() && instance.isAllocated(buf)) 
        instance.free(buf);
    else
        ::free(buf);
}

}
