#pragma once

#include <Common/ArrayCache.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <atomic>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/core/noncopyable.hpp>

namespace DB
{

template <typename Key>
class BlockCache : private boost::noncopyable
{
private:
    struct ChunkStorageListTag;

    using ChunkStorageListHook = boost::intrusive::list_base_hook<boost::intrusive::tag<ChunkStorageListTag>>;

    struct Chunk : public ChunkStorageListHook, private boost::noncopyable
    {
        void * ptr;
        size_t size;

        Chunk(Chunk && other) noexcept : ptr(other.ptr), size(other.size)
        {
            other.ptr = nullptr;
        }

        static Chunk * create(size_t size_)
        {
            return new Chunk(size_);
        }

        void destroy()
        {
            delete this;
        }

    private:
        explicit Chunk(size_t size_);
        ~Chunk();
    };

    using ChunkStorageList = boost::intrusive::list<Chunk,
        boost::intrusive::base_hook<ChunkStorageListHook>, boost::intrusive::constant_time_size<true>>;

    /** Invariants:
     * acquired_chunks contains all chunks which contain at least one non-free region
     * free_chunks contains all chunks which contain exactly once free region
     */
    ChunkStorageList acquired_chunks;
    ChunkStorageList free_chunks; 

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
        Chunk * chunk;

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

        [[nodiscard]] bool isChunkOwner() const
        {
            return chunk != nullptr && ptr == chunk->ptr && chunk->size == size;
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

    size_t max_total_size = 0;
    size_t max_size_to_evict_on_purging = 0;

    /// We will allocate memory in chunks of at least that size.
    /// 64 MB makes mmap overhead comparable to memory throughput.
    static constexpr size_t min_chunk_size = 64 * 1024 * 1024;

    static constexpr size_t alignment = 16;

    size_t total_chunks_size = 0;
    std::atomic<size_t> total_useful_cache_size = 0;
    std::atomic<size_t> total_useful_cache_count = 0;
 
    RegionMetadata * addNewChunk(Chunk * chunk);
    RegionMetadata * allocateNewChunk(size_t size);
    RegionMetadata * allocateFromFreeRegion(RegionMetadata * free_region, size_t size);

    RegionMetadata * allocate(size_t size);
    void freeRegion(RegionMetadata * region) noexcept;

    void evictRegion(RegionMetadata * evicted_region) noexcept;
    RegionMetadata * evictSome(size_t requested_size) noexcept;

    void shrink(size_t max_size_to_evict);

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

    BlockCache() = default;

    explicit BlockCache(size_t max_total_size_) : max_total_size(max_total_size_)
    {
    }

    ~BlockCache();

    void initialize(size_t max_total_size_, size_t max_size_to_evict_on_purging_) 
    { 
        max_total_size = max_total_size_; 
        max_size_to_evict_on_purging = max_size_to_evict_on_purging_;
    }

    /// Applications isn't supposed to call these methods directly without 
    /// proxy classes to hold raw pointers
    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, 
                       GetSizeFunc && get_size, 
                       InitializeFunc && initialize, 
                       bool * was_calculated)
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
        RegionMetadata * region = allocate(size);

        if (!region) 
            return {};

        region->key = key;

        try
        {
            initialize(region->ptr);
        }
        catch (...)
        {
            freeRegion(region);
            throw;
        }

        key_map.insert(*region);
        if (was_calculated)
            *was_calculated = true;

        /// Statistics
        if (region) {
            std::cerr << "Size: " << size << " region size: " << region->size << std::endl;
            total_useful_cache_size.fetch_add(region->size, std::memory_order_relaxed);
            total_useful_cache_count.fetch_add(1, std::memory_order_relaxed);
        }

        return new Holder(*this, *region, cache_lock);
    }

    HolderPtr get(const Key & key);

    void reset();

    void purge();

    bool isValid() const { return max_total_size != 0; }

    size_t getCacheWeight() const;

    size_t getCacheCount() const;

    ChunkStorageList takeChunks(size_t max_size_to_evict);

    void addNewChunks(ChunkStorageList & chunk_list);
};

template <typename Key>
class GlobalBlockCache {
public:
    static BlockCache<Key> & instance()
    {
        static GlobalBlockCache<Key> holder;
        return holder.block_cache;
    }

private:
    BlockCache<Key> block_cache;

    GlobalBlockCache() = default;
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

    UnifiedCacheAdapter() : instance(GlobalBlockCache<Key>::instance())
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
        instance.reset();
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

}
