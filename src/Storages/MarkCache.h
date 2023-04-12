#pragma once

#include <memory>

#include <Common/UnifiedCache.h>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <Formats/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
}

namespace DB
{

class MarkCache : public UnifiedCacheAdapter<UInt128, MarksInCompressedFile> 
{
private:
    using Base = UnifiedCacheAdapter<UInt128, MarksInCompressedFile>;
    using HolderPtr = Base::CachePayloadHolderPtr;

public:
    // TODO: Remove size and policy parameters from the constructor
    explicit MarkCache(size_t /*max_size_in_bytes*/, const String & /*mark_cache_policy*/ = "") 
    {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key);

        return key;
    }

    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, GetSizeFunc && get_size, InitializeFunc && initialize)
    {
        bool was_calculated = false;
        auto result = Base::getOrSet(key, std::forward<GetSizeFunc>(get_size), std::forward<InitializeFunc>(initialize), &was_calculated);
        if (was_calculated)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;

}
