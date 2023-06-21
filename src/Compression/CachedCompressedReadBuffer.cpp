#include "CachedCompressedReadBuffer.h"

#include <IO/WriteHelpers.h>
#include <Compression/LZ4_decompress_faster.h>
#include "IO/UncompressedCache.h"

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


void CachedCompressedReadBuffer::initInput()
{
    if (!file_in)
    {
        file_in = file_in_creator();
        compressed_in = file_in.get();

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }
}


void CachedCompressedReadBuffer::prefetch()
{
    initInput();
    file_in->prefetch();
}


bool CachedCompressedReadBuffer::nextImpl()
{
    /// Let's check for the presence of a decompressed block in the cache, grab the ownership of this block, if it exists.
    UInt128 key = cache->hash(path, file_pos);

    size_t size_compressed = 0;
    size_t size_decompressed = 0;
    size_t size_compressed_without_checksum = 0;
    size_t additional_bytes = 0;

    auto get_size = [&]() -> size_t
    {
        initInput();
        file_in->seek(file_pos, SEEK_SET);

        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum, false);   
        if (size_compressed > 0) 
        {
            additional_bytes = codec->getAdditionalSizeAtTheEndOfBuffer();
            return size_decompressed + additional_bytes;
        }
        else 
        {
            return 0;
        }
    };

    auto initialize = [&](void * ptr)
    {
        auto * data_ptr = reinterpret_cast<char *>(ptr) + sizeof(UncompressedCacheCell);
        auto * uncompressed_payload = new (ptr) UncompressedCacheCell();
        uncompressed_payload->data = data_ptr;
        uncompressed_payload->data_size = size_decompressed + additional_bytes;
        uncompressed_payload->compressed_size = size_compressed;
        uncompressed_payload->additional_bytes = additional_bytes;

        decompressTo(data_ptr, size_decompressed, size_compressed_without_checksum);
    };

    owned_region = cache->getOrSet(key, get_size, initialize);

    auto & owned_cell = owned_region->payload();

    if (owned_cell.data_size == 0)
        return false;

    working_buffer = Buffer(owned_cell.data, owned_cell.data + owned_cell.data_size - owned_cell.additional_bytes);

    /// nextimpl_working_buffer_offset is set in the seek function (lazy seek). So we have to
    /// check that we are not seeking beyond working buffer.
    if (nextimpl_working_buffer_offset > working_buffer.size())
        throw Exception("Seek position is beyond the decompressed block"
        " (pos: " + toString(nextimpl_working_buffer_offset) + ", block size: " + toString(working_buffer.size()) + ")",
        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    file_pos += owned_cell.compressed_size;

    return true;
}

CachedCompressedReadBuffer::CachedCompressedReadBuffer(
    const std::string & path_, std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator_, UncompressedCache * cache_, bool allow_different_codecs_)
    : ReadBuffer(nullptr, 0), file_in_creator(std::move(file_in_creator_)), cache(cache_), path(path_), file_pos(0)
{
    allow_different_codecs = allow_different_codecs_;
}

void CachedCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    /// Nothing to do if we already at required position
    if (!owned_region && file_pos == offset_in_compressed_file
        && ((!buffer().empty() && offset() == offset_in_decompressed_block) ||
            nextimpl_working_buffer_offset == offset_in_decompressed_block))
        return;

    if (owned_region &&
        offset_in_compressed_file == file_pos - owned_region->payload().compressed_size &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        pos = working_buffer.begin() + offset_in_decompressed_block;
    }
    else
    {
        /// Remember position in compressed file (will be moved in nextImpl)
        file_pos = offset_in_compressed_file;
        /// We will discard our working_buffer, but have to account rest bytes
        bytes += offset();
        /// No data, everything discarded
        resetWorkingBuffer();
        owned_region.reset();

        /// Remember required offset in decompressed block which will be set in
        /// the next ReadBuffer::next() call
        nextimpl_working_buffer_offset = offset_in_decompressed_block;
    }
}

}
