#pragma once
#include <cstdio>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <new>
#include <base/defines.h>

#include <Common/Concepts.h>
#include <Common/CurrentMemoryTracker.h>
#include "config.h"

#include <Common/UnifiedCache.h>

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC
#    include <cstdlib>
#endif

namespace Memory
{

inline ALWAYS_INLINE size_t alignToSizeT(std::align_val_t align) noexcept
{
    return static_cast<size_t>(align);
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void * newImpl(std::size_t size, TAlign... align)
{
    void * ptr = nullptr;
    auto & instance = DB::BuddyArena::instance();
    if constexpr (sizeof...(TAlign) == 1)
    {
        if (size >= DB::UNIFIED_ALLOC_THRESHOLD && instance.isValid())
            ptr = instance.malloc(size, alignToSizeT(align...));
        else
            ptr = aligned_alloc(alignToSizeT(align...), size);
    } 
    else
    {
        if (size >= DB::UNIFIED_ALLOC_THRESHOLD && instance.isValid())
            ptr = instance.malloc(size);
        else
            ptr = malloc(size);
    }

    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
    auto & instance = DB::BuddyArena::instance();
    if (size >= DB::UNIFIED_ALLOC_THRESHOLD && instance.isValid())
        return instance.malloc(size);
    else
        return malloc(size);
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size, std::align_val_t align) noexcept
{
    auto & instance = DB::BuddyArena::instance();
    if (size >= DB::UNIFIED_ALLOC_THRESHOLD && instance.isValid())
        return instance.malloc(size, static_cast<size_t>(align));
    else 
        return aligned_alloc(static_cast<size_t>(align), size);
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    auto & instance = DB::BuddyArena::instance();
    if (instance.isValid() && instance.isAllocated(ptr))
        instance.free(ptr);
    else
        free(ptr);
}

#if USE_JEMALLOC

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size, TAlign... align) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    auto & instance = DB::BuddyArena::instance();
    if (instance.isValid() && instance.isAllocated(ptr)) 
    {
        instance.free(ptr);
    }
    else 
    {
        if constexpr (sizeof...(TAlign) == 1)
            sdallocx(ptr, size, MALLOCX_ALIGN(alignToSizeT(align...)));
        else
            sdallocx(ptr, size, 0);

    }
}

#else

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]], TAlign... /* align */) noexcept
{
    free(ptr);
}

#endif

#if defined(OS_LINUX)
#    include <malloc.h>
#elif defined(OS_DARWIN)
#    include <malloc/malloc.h>
#endif

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size, TAlign... align [[maybe_unused]])
{
    size_t actual_size = size;

#if USE_JEMALLOC
    /// The nallocx() function allocates no memory, but it performs the same size computation as the mallocx() function
    /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
    if (likely(size != 0))
    {
        if constexpr (sizeof...(TAlign) == 1)
            actual_size = nallocx(size, MALLOCX_ALIGN(alignToSizeT(align...)));
        else
            actual_size = nallocx(size, 0);
    }
#endif

    return actual_size;
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void trackMemory(std::size_t size, TAlign... align)
{
    std::size_t actual_size = getActualAllocationSize(size, align...);
    CurrentMemoryTracker::allocNoThrow(actual_size);
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]] = 0, TAlign... align [[maybe_unused]]) noexcept
{
    auto & instance = DB::BuddyArena::instance();
    if (instance.isValid() && instance.isAllocated(ptr))  {
        CurrentMemoryTracker::free(size);
        return;
    }

    try
    {
#if USE_JEMALLOC

        /// @note It's also possible to use je_malloc_usable_size() here.
        if (likely(ptr != nullptr))
        {
            if constexpr (sizeof...(TAlign) == 1)
                CurrentMemoryTracker::free(sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align...))));
            else
                CurrentMemoryTracker::free(sallocx(ptr, 0));
        }
#else
        if (size)
            CurrentMemoryTracker::free(size);
#    if defined(_GNU_SOURCE)
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        else
            CurrentMemoryTracker::free(malloc_usable_size(ptr));
#    endif
#endif
    }
    catch (...)
    {
    }
}

}

#pragma GCC diagnostic pop
