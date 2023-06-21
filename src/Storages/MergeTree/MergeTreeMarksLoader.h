#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MarkCache.h>
#include <IO/ReadSettings.h>
#include <Common/UnifiedCache.h>
#include <Common/ThreadPool.h>
#include <Formats/MarkInCompressedFile.h>


namespace DB
{

struct MergeTreeIndexGranularityInfo;
class Threadpool;

class MergeTreeMarksLoader
{
public:
    using SharedMarksHolder = SharedPayloadHolder<MarksInCompressedFile>;
    using MarksHolder = PayloadHolder<MarksInCompressedFile>;
    using MarksHolderPtr = std::shared_ptr<MarksHolder>;

    MergeTreeMarksLoader(
        DataPartStoragePtr data_part_storage_,
        MarkCache * mark_cache_,
        const String & mrk_path,
        size_t marks_count_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        bool save_marks_in_cache_,
        const ReadSettings & read_settings_,
        ThreadPool * load_marks_threadpool_,
        size_t columns_in_mark_ = 1);

    ~MergeTreeMarksLoader();

    const MarkInCompressedFile & getMark(size_t row_index, size_t column_index = 0);

private:
    DataPartStoragePtr data_part_storage;
    MarkCache * mark_cache = nullptr;
    String mrk_path;
    size_t marks_count;
    const MergeTreeIndexGranularityInfo & index_granularity_info;
    bool save_marks_in_cache = false;
    size_t columns_in_mark;
    MarksHolderPtr marks;  
    ReadSettings read_settings;

    void readIntoMarks(MarksInCompressedFile & marks_to_load);
    MarksHolderPtr loadMarksImpl();
    MarksHolderPtr loadMarks();
    std::future<MarksHolderPtr> loadMarksAsync();

    std::future<MarksHolderPtr> future;
    ThreadPool * load_marks_threadpool;
};

}
