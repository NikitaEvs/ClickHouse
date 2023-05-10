#pragma once

#include <Common/ArrayCache.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/core/noncopyable.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


template <typename Key>
class IRebalanceStrategy;


struct MemoryBlockListTag;
using MemoryBlockListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<MemoryBlockListTag>>;
struct MemoryBlock : public MemoryBlockListHook, private boost::noncopyable
{
    void * ptr;
    size_t size;

    MemoryBlock(MemoryBlock && other) noexcept : ptr(other.ptr), size(other.size)
    {
        other.ptr = nullptr;
    }

    static MemoryBlock * create(size_t size_)
    {
        return new MemoryBlock(size_);
    }

    void destroy()
    {
        delete this;
    }
private:
    explicit MemoryBlock(size_t size_);
    ~MemoryBlock();
};
using MemoryBlockList = boost::intrusive::list<MemoryBlock,
    boost::intrusive::base_hook<MemoryBlockListHook>, boost::intrusive::constant_time_size<true>>;


template <typename Key>
class BlockCache : private boost::noncopyable
{
private:
    /** Invariants:
     * acquired_memory_blocks contains all blocks which contain at least one non-free region
     * free_memory_blocks contains all blocks which contain exactly once free region
     */
    MemoryBlockList acquired_memory_blocks;
    MemoryBlockList free_memory_blocks; 

    // using Chunks = std::list<Chunk>;
    // Chunks chunks;

    struct LRUListTag;
    struct AdjacencyListTag;
    struct SizeMultimapTag;
    struct KeyMapTag;

    using LRUListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<LRUListTag>>;
    using AdjacencyListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<AdjacencyListTag>>;
    using SizeMultimapHook = boost::intrusive::set_base_hook<boost::intrusive::tag<SizeMultimapTag>>;
    using KeyMapHook = boost::intrusive::set_base_hook<boost::intrusive::tag<KeyMapTag>>;

    struct RegionMetadata : public LRUListHook, AdjacencyListHook, SizeMultimapHook, KeyMapHook
    {
        Key key;

        union
        {
            void * ptr;
            char * char_ptr;
        };
        size_t size;
        size_t refcount = 0;
        MemoryBlock * memory_block;

        bool operator< (const RegionMetadata & other) const { return size < other.size; }

        bool isFree() const { return SizeMultimapHook::is_linked(); }

        static RegionMetadata * create()
        {
            return new RegionMetadata;
        }

        void destroy()
        {
            delete this;
        }

        [[nodiscard]] bool isBlockOwner() const
        {
            return memory_block != nullptr && ptr == memory_block->ptr && memory_block->size == size;
        }

    private:
        RegionMetadata() = default;
        ~RegionMetadata() = default;
    };

    struct RegionCompareBySize
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const { return a.size < b.size; }
        bool operator() (const RegionMetadata & a, size_t size) const { return a.size < size; }
        bool operator() (size_t size, const RegionMetadata & b) const { return size < b.size; }
    };

    struct RegionCompareByKey
    {
        bool operator() (const RegionMetadata & a, const RegionMetadata & b) const { return a.key < b.key; }
        bool operator() (const RegionMetadata & a, Key key) const { return a.key < key; }
        bool operator() (Key key, const RegionMetadata & b) const { return key < b.key; }
    };

    // TODO: boost::intrusize::unordered_set?
    using LRUList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<LRUListHook>, boost::intrusive::constant_time_size<true>>;
    using AdjacencyList = boost::intrusive::list<RegionMetadata,
        boost::intrusive::base_hook<AdjacencyListHook>, boost::intrusive::constant_time_size<true>>;
    using SizeMultimap = boost::intrusive::multiset<RegionMetadata,
        boost::intrusive::compare<RegionCompareBySize>, boost::intrusive::base_hook<SizeMultimapHook>, boost::intrusive::constant_time_size<true>>;
    using KeyMap = boost::intrusive::set<RegionMetadata,
        boost::intrusive::compare<RegionCompareByKey>, boost::intrusive::base_hook<KeyMapHook>, boost::intrusive::constant_time_size<true>>;

    /** Each region could be:
      * - free: not holding any data;
      * - allocated: having data, addressed by key;
      * -- allocated, in use: holded externally, could not be evicted;
      * -- allocated, not in use: not holded, could be evicted.
      */

    /** Invariants:
      * adjacency_list contains all regions
      * size_multimap contains free regions
      * key_map contains allocated regions
      * lru_list contains allocated regions, that are not in use
      */

    LRUList lru_list;
    AdjacencyList adjacency_list;
    SizeMultimap size_multimap;
    KeyMap key_map;

    mutable std::mutex mutex;

    /// TODO: Remove temp workaround, add single flight system
    mutable std::mutex get_or_set_mutex;

    // size_t max_total_size = 0;
    // size_t max_size_to_evict_on_purging = 0;

    /// We will allocate memory in blocks of at least that size.
    /// 64 MB makes mmap overhead comparable to memory throughput.
    static constexpr size_t min_block_size = 64 * 1024 * 1024;

    static constexpr size_t alignment = 16;

    size_t total_memory_blocks_size = 0;
    std::atomic<size_t> total_useful_cache_size = 0;
    std::atomic<size_t> total_useful_cache_count = 0;

    /// Block cache identifier used in the rebalance_strategy
    const std::string name;

    using RebalanceStrategy = IRebalanceStrategy<Key>;
    std::shared_ptr<RebalanceStrategy> rebalance_strategy;
 
    RegionMetadata * addNewRegion(MemoryBlock & memory_block, std::lock_guard<std::mutex> & cache_lock);

    /// Precondition: free_region.size >= size.
    RegionMetadata * allocateFromFreeRegion(RegionMetadata * free_region, size_t size, std::lock_guard<std::mutex> & cache_lock);

    RegionMetadata * allocate(size_t size, std::lock_guard<std::mutex> & cache_lock);

    /// Precondition: region is not in lru_list, not in key_map, not in size_multimap.
    /// Postcondition: region is not in lru_list, not in key_map,
    ///  inserted into size_multimap, possibly coalesced with adjacent free regions.
    void freeRegion(RegionMetadata * region, std::lock_guard<std::mutex> & cache_lock) noexcept;

    void evictRegion(RegionMetadata * evicted_region, std::lock_guard<std::mutex> & cache_lock) noexcept;

    /// Evict region from cache and return it, coalesced with nearby free regions.
    /// While size is not enough, evict adjacent regions at right, if any.
    /// If nothing to evict, returns nullptr.
    /// Region is removed from lru_list and key_map and inserted into size_multimap.
    RegionMetadata * evictSome(size_t requested_size, std::lock_guard<std::mutex> & cache_lock) noexcept;

    // void shrink(size_t max_size_to_evict);

public:
    struct Holder : private boost::noncopyable
    {
        Holder(BlockCache & cache_, RegionMetadata & region_, std::lock_guard<std::mutex>&);
        ~Holder();

        void * ptr() 
        { 
            assert(region.ptr != nullptr);
            return region.ptr; 
        }
        const void * ptr() const 
        { 
            assert(region.ptr != nullptr);
            return region.ptr; 
        }
        size_t size() const { return region.size; }
        Key key() const { return region.key; }

    private:
        BlockCache & cache;
        RegionMetadata & region;
        void * region_ptr;
    };

    using HolderPtr = Holder *;

    BlockCache(const std::string & name_, std::shared_ptr<RebalanceStrategy> rebalance_strategy_);

    ~BlockCache();

    /// Applications isn't supposed to call these methods directly without 
    /// proxy classes to hold raw pointers
    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, 
                       GetSizeFunc && get_size, 
                       InitializeFunc && initialize, 
                       bool * was_calculated)
    {
        /// TODO: Remove temp hack, add single-flight system
        std::lock_guard get_or_set_lock(get_or_set_mutex);
        RegionMetadata * region;
        {
            std::lock_guard cache_lock(mutex);

            auto it = key_map.find(key, RegionCompareByKey());
            if (key_map.end() != it)
            {
                if (was_calculated)
                    *was_calculated = false;
                return new Holder(*this, *it, cache_lock);
            }

            size_t size = get_size();
            region = allocate(size, cache_lock);

            if (!region) 
                return {};

            region->key = key;
        }

        try
        {
            initialize(region->ptr);
        }
        catch (...)
        {
            std::lock_guard cache_lock(mutex);
            freeRegion(region, cache_lock);
            throw;
        }

        std::lock_guard cache_lock(mutex);
        key_map.insert(*region);
        if (was_calculated)
            *was_calculated = true;

        /// Statistics
        if (region) {
            total_useful_cache_size.fetch_add(region->size, std::memory_order_relaxed);
            total_useful_cache_count.fetch_add(1, std::memory_order_relaxed);
        }

        return new Holder(*this, *region, cache_lock);
    }

    HolderPtr get(const Key & key);

    size_t getCacheWeight() const;

    size_t getCacheCount() const;

    MemoryBlockList takeMemoryBlocks(size_t max_size_to_evict, std::lock_guard<std::mutex> & cache_lock);
    MemoryBlockList takeMemoryBlocks(size_t max_size_to_evict);

    void addNewMemoryBlocks(MemoryBlockList & memory_blocks, std::lock_guard<std::mutex> & cache_lock);
    void addNewMemoryBlocks(MemoryBlockList & memory_blocks);
};


/// Lifetime of the IRebalanceStrategy is the same as the lifetime of the BlockCachesManager
template <typename Key>
class IRebalanceStrategy 
{
public:
    using BlockCacheMapping = std::unordered_map<std::string, BlockCache<Key>>;
    explicit IRebalanceStrategy(BlockCacheMapping & caches_) : caches(caches_)
    {}

    virtual ~IRebalanceStrategy() = default;

    /// Interfaces for the BlockCache
    virtual MemoryBlockList initialize(const std::string & caller_name) = 0;
    virtual void shouldRebalance(const std::string & caller_name, size_t new_item_size, std::lock_guard<std::mutex> & cache_lock) = 0;
    virtual void finalize(const std::string & caller_name, MemoryBlockList memory_blocks) = 0;

    /// Interfaces for the BlockCacheManager
    virtual size_t getWeight() const = 0;
    virtual size_t getCount() const = 0;
    virtual void purge() = 0;
    virtual void reset() = 0;

protected:
    BlockCacheMapping & caches;
};


struct BlockCacheSettings
{
    size_t cache_max_size;
    size_t max_size_to_evict_on_purging;
};
using BlockCacheSettingsMapping = std::unordered_map<std::string, BlockCacheSettings>;


template <typename Key>
class DummyRebalanceStrategy : public IRebalanceStrategy<Key>
{
    using Base = IRebalanceStrategy<Key>;
    using BlockCacheMapping = typename IRebalanceStrategy<Key>::BlockCacheMapping;
    using BlockCache = BlockCache<Key>;

public:
    struct BlockCacheStats 
    {
        size_t total_memory_blocks_size = 0;
        size_t max_total_size = 0;
        size_t max_size_to_evict_on_purging = 0;

        explicit BlockCacheStats(const BlockCacheSettings & settings)
            : max_total_size(settings.cache_max_size)
            , max_size_to_evict_on_purging(settings.max_size_to_evict_on_purging)
        {}

        BlockCacheStats() = default;
    };
    using BlockCacheStatsMapping = std::unordered_map<std::string, BlockCacheStats>;

    DummyRebalanceStrategy(BlockCacheMapping & caches_, const BlockCacheSettingsMapping & cache_settings);

    MemoryBlockList initialize(const std::string & caller_name) final;

    void shouldRebalance(const std::string & caller_name, size_t new_item_size, std::lock_guard<std::mutex> & cache_lock) final;

    void finalize(const std::string & caller_name, MemoryBlockList memory_blocks) final;

    /// Get a size of the useful data in the cache
    /// Note: can return out-of-thin-air values for the sum of cache weights
    /// Use only for the statistics purpose
    size_t getWeight() const final;

    /// Get a number of useful items in the cache
    /// Note: can return out-of-thin-air values for the sum of cache weights
    /// Use only for the statistics purpose
    size_t getCount() const final;

    void purge() final;

    void reset() final;

private:
    BlockCacheStatsMapping block_cache_stats;

    /// We will allocate memory in blocks of at least that size.
    /// 64 MB makes mmap overhead comparable to memory throughput.
    static constexpr size_t min_memory_block_size = 64 * 1024 * 1024;

    static size_t roundUp(size_t x, size_t rounding)
    {
        return (x + (rounding - 1)) / rounding * rounding;
    }
};


template <typename Key>
class BuddyRebalanceStrategy : public IRebalanceStrategy<Key>
{
    using Base = IRebalanceStrategy<Key>;
    using BlockCacheMapping = typename IRebalanceStrategy<Key>::BlockCacheMapping;
    using BlockCache = BlockCache<Key>;

public:
    struct BlockCacheStats 
    {
        size_t total_memory_blocks_size = 0;
        size_t max_total_size = 0;
        size_t max_size_to_evict_on_purging = 0;

        explicit BlockCacheStats(const BlockCacheSettings & settings)
            : max_total_size(settings.cache_max_size)
            , max_size_to_evict_on_purging(settings.max_size_to_evict_on_purging)
        {}

        BlockCacheStats() = default;
    };
    using BlockCacheStatsMapping = std::unordered_map<std::string, BlockCacheStats>;

    BuddyRebalanceStrategy(BlockCacheMapping & caches_, const BlockCacheSettingsMapping & cache_settings);

    MemoryBlockList initialize(const std::string & caller_name) final;

    void shouldRebalance(const std::string & caller_name, size_t new_item_size, std::lock_guard<std::mutex> & cache_lock) final;

    void finalize(const std::string & caller_name, MemoryBlockList memory_blocks) final;

    /// Get a size of the useful data in the cache
    /// Note: can return out-of-thin-air values for the sum of cache weights
    /// Use only for the statistics purpose
    size_t getWeight() const final;

    /// Get a number of useful items in the cache
    /// Note: can return out-of-thin-air values for the sum of cache weights
    /// Use only for the statistics purpose
    size_t getCount() const final;

    void purge() final;

    void reset() final;

private:
    BlockCacheStatsMapping block_cache_stats;
};


template <typename Key>
class BlockCachesManager 
{
    using RebalanceStrategy = IRebalanceStrategy<Key>;
    using BlockCacheMapping = typename RebalanceStrategy::BlockCacheMapping;

public:
    static BlockCachesManager<Key> & instance()
    {
        static BlockCachesManager<Key> manager;
        return manager;
    }

    void initialize(
        const std::vector<std::string> & block_cache_names,
        std::string_view rebalance_strategy_name = "",
        const BlockCacheSettingsMapping & cache_settings = {});

    /// block_caches hashmap doesn't change since initialize function so we can access it wihtout locking
    BlockCache<Key> & getBlockCacheInstance(const std::string & name);

    size_t getCacheWeight() const;

    size_t getCacheCount() const;

    void purge();

    void reset();

private:
    BlockCacheMapping block_caches;
    std::shared_ptr<RebalanceStrategy> rebalance_strategy;

    static constexpr std::string_view default_rebalance_strategy = "dummy";

    BlockCachesManager() = default;
};


// Unified interface for the access to items from the cache and items not from the cache
template <typename Payload>
struct PayloadHolder 
{
    PayloadHolder() = default;
    virtual ~PayloadHolder() = default;

    virtual Payload & payload() = 0;
    virtual const Payload & payload() const = 0;
    Payload & operator*() { return payload(); }
    const Payload & operator*() const { return payload(); }

    /// PayloadHolder should be light-weight and copy- and move-constuctible
    PayloadHolder(const PayloadHolder&) = default;
    PayloadHolder& operator=(const PayloadHolder&) = default;
    PayloadHolder(PayloadHolder&&) noexcept = default;
    PayloadHolder& operator=(PayloadHolder&&) noexcept = default;
};


// Simple holder for the pointer with an unified interface with CachePayloadHolder
// Used to unify access to the cache items and non-cache items
template <typename Payload>
struct SharedPayloadHolder : public PayloadHolder<Payload> 
{
    /// Adopt payload_ptr_ and own a memory for it
    explicit SharedPayloadHolder(Payload * payload_ptr_)
        : payload_ptr(payload_ptr_) 
    {}

    ~SharedPayloadHolder() override { delete payload_ptr; }

    Payload & payload() override { return *payload_ptr; }
    const Payload & payload() const override { return *payload_ptr; }
private:
    Payload * payload_ptr;
};


template <typename TKey, typename Payload>
class UnifiedCacheAdapter 
{
public:
    using Key = TKey; 
    using HolderPtr = typename BlockCache<Key>::HolderPtr;

    struct CachePayloadHolder : public PayloadHolder<Payload> {
        /// Adopt holder_ptr_ and own a reference to the memory for it
        explicit CachePayloadHolder(HolderPtr holder_ptr_)
            : payload_ptr(reinterpret_cast<Payload *>(holder_ptr_->ptr()))
            , holder_ptr(std::move(holder_ptr_))
        {}

        ~CachePayloadHolder() override { delete holder_ptr; }

        Payload & payload() override { return *payload_ptr; }
        const Payload & payload() const override { return *payload_ptr; }
    private:
        Payload * payload_ptr;
        HolderPtr holder_ptr;
    };

    using CachePayloadHolderPtr = std::shared_ptr<CachePayloadHolder>;

    explicit UnifiedCacheAdapter(const std::string & block_cache_name) 
        : instance(BlockCachesManager<Key>::instance().getBlockCacheInstance(block_cache_name))
    {
    }

    /// initialize must construct Payload object in the start of the memory block
    /// get_size must return number of additional bytes that is needed fro the object (except for the object initialization)
    template <typename GetSizeFunc, typename InitializeFunc>
    CachePayloadHolderPtr getOrSet(const Key & key, 
                              GetSizeFunc && get_size, 
                              InitializeFunc && initialize, 
                              bool * was_calculated) 
    {
        auto get_full_size = [&get_size]() 
        {
            return sizeof(Payload) + get_size();
        };
        auto * holder_ptr = instance.getOrSet(key, get_full_size, initialize, was_calculated);
        if (holder_ptr == nullptr) 
            return nullptr;
        else
            return std::make_shared<CachePayloadHolder>(holder_ptr);
    }

    CachePayloadHolderPtr get(const Key & key)
    {
        auto * holder_ptr = instance.get(key);
        return std::make_shared<CachePayloadHolder>(holder_ptr);
    }

    void reset()
    {
        /// TODO: implement support for the resetting some specific type of cache
        BlockCachesManager<Key>::instance().reset();
    }

    size_t weight() const
    {
        /// TODO: implement
        return 0;
    }

    size_t count() const
    {
        /// TODO: implement
        return 0;
    }

private:
    BlockCache<Key> & instance;
};


extern template class BlockCache<UInt128>;
extern template class DummyRebalanceStrategy<UInt128>;
extern template class BlockCachesManager<UInt128>;

}
