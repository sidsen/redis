// tmalloc.h
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera
//
//
// Thread-local memory allocator.
// To use, include this file and link tmalloc.cpp to your application.
//
//     Each thread keeps its own pool of memory. To allocate a buffer, a thread gets memory from its pool.
// If the same thread who allocated the buffer later frees the buffer, the buffer is returned
// to the local pool. If, however, a different thread frees the buffer, the buffer should not be
// returned to the thread's local pool, otherwise memory from one thread's pool starts to move
// to another thread's pool. In some applications where some threads allocate and produce data
// while others consume a free data, this would be a problem as memory would continually flow
// from one thread to another, eventually exhausting the pool of the first thread.
//     To address this problem, if a thread frees a buffer allocated by another thread, it
// will send back the buffer to the other thread so that the other thread can return the buffer
// to its local pool. To send back buffers efficiently, a thread accumulates a bunch of
// buffers into a superbuffer of buffers, and once the superbuffer is large enough,
// it sends the entire superbuffer. The reason for doing this is to avoid too much thread
// synchronization.
//     When a thread allocates memory, it checks to see if any superbuffers are being
// returned to it. If so, it returns all the buffers in the superbuffers to the local pool.
//     To send superbuffers between threads, there is a concurrent linked list per thread.
// The list stores the superbuffers to be received by the thread. Another thread
// adds a superbuffer to the link list of the first thread by create a new node pointing
// to the current hand, and then doing a compare-and-swap on the current head to
// change it to the new head. If the compare-and-swap fails, someone else managed to
// modify the head, so the process is repeated. The thread will consume the superbuffers
// in the link list, skipping the first entry. Doing so allows the thread to consume
// the superbuffers without changing the head pointer, and hence without contending
// with threads trying to add new elements. The side effect of this is that the
// superbuffer at the head will be consumed only after a new superbuffer is added.
//     The local pool is itself a set of subpools, where each subpool keeps
// buffers of a fixed size. The sizes of the subpools grow exponentially. A new
// buffer is allocated from the subpool holding the smallest buffers that will
// fit the requested size.

#ifndef __TMALLOC_H
#define __TMALLOC_H

#ifdef __cplusplus
#define TMALLOC_EXPORT extern "C" 
#else
#define TMALLOC_EXPORT
#endif

TMALLOC_EXPORT void *_tmalloc(size_t size);
TMALLOC_EXPORT void _tfree(void *buf);
TMALLOC_EXPORT void *_trealloc(void *ptr, size_t size);
TMALLOC_EXPORT void *_tcalloc(size_t n_elements, size_t elem_size);
TMALLOC_EXPORT size_t _tgetsize(void *buf);
void _tmreportprocessor(int processor); // reports on which processor a thread is running. Used only if _TM_NUMAALLOC is defined

// original allocation functions
void *orig_malloc(size_t size);
void orig_free(void *buf);
void *orig_realloc(void *ptr, size_t size);

/* Commenting out as redis needs to define this in the right place to avoid 
   naming conflicts */
//#define malloc _tmalloc
//#define free _tfree
//#define realloc _trealloc

#endif