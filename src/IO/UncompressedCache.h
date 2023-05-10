#pragma once

#include <Common/UnifiedCache.h>
#include <Common/SipHash.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/CacheBase.h>


namespace ProfileEvents
{
    extern const Event UncompressedCacheHits;
    extern const Event UncompressedCacheMisses;
    extern const Event UncompressedCacheWeightLost;
}

namespace DB
{


struct UncompressedCacheCell
{
    char * data;
    size_t data_size;
    size_t compressed_size;
    UInt32 additional_bytes;
};


class UncompressedCache : public UnifiedCacheAdapter<UInt128, UncompressedCacheCell> 
{
private:
    using Base = UnifiedCacheAdapter<UInt128, UncompressedCacheCell>;
    using HolderPtr = Base::CachePayloadHolderPtr;

public:
    explicit UncompressedCache(const String & block_cache_name) : Base(block_cache_name)
    {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.get128(key);

        return key;
    }

    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, GetSizeFunc && get_size, InitializeFunc && initialize)
    {
        bool was_calculated = false;
        auto result = Base::getOrSet(key, std::forward<GetSizeFunc>(get_size), std::forward<InitializeFunc>(initialize), &was_calculated);

        if (was_calculated)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);

        return result;
    }

// TODO: Add support for the ProfileEvents::UncompressedCacheWeightLost
// private:
//     void onRemoveOverflowWeightLoss(size_t weight_loss) override
//     {
//         ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
//     }
};



using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
