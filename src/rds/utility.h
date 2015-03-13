#ifndef _UTILITY_H
#define _UTILITY_H

// utility.h : Common utility classes and definitions.
//


//#ifdef _MSC_VER
//#pragma once
//#define _CRT_SECURE_NO_WARNINGS
//#pragma warning(disable: 4996)
//#endif

#ifdef _WIN32
#define inline __inline
#include <stdbool.h>
#include <stddef.h>
#endif

#ifdef _MSC_VER

// these definitions for fetch-and-add, atomic-increment, and atomic-decrement are compiler specific
#define FetchAndAdd32(ptr32, val32) _InterlockedExchangeAdd((long*)ptr32,val32)
#define FetchAndAdd64(ptr64, val64) _InterlockedExchangeAdd64((long*)ptr64,val64)
#define FetchSwapPtr(ptr, val) _InterlockedExchangePointer((void* volatile*)ptr,val)
#define AtomicInc32(ptr32) _InterlockedIncrement((long*)ptr32)
#define AtomicInc64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define AtomicDec32(ptr32) _InterlockedDecrement((long*)ptr32)
#define AtomicDec64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define CompareSwap32(ptr32,cmp32,val32) _InterlockedCompareExchange(ptr32,val32,cmp32)
#define CompareSwap64(ptr64,cmp64,val64) _InterlockedCompareExchange(ptr64,val64,cmp64)
#define CompareSwapPtr(ptr,cmp,val) _InterlockedCompareExchangePointer((void* volatile*)ptr,val,cmp)


// definitions to align a variable in 4-byte or 64-byte boundaries, also compiler specific
#define Align4 __declspec(align(4))
#define Align8 __declspec(align(8))
#define Align16 __declspec(align(16))
#define Align64 __declspec(align(64))

// defines a thread local variable
#define Tlocal __declspec(thread) 
#define Tlocal4 __declspec(thread,align(4))
#define Tlocal8 __declspec(thread,align(8))
#define Tlocal16 __declspec(thread,align(16))
#define Tlocal64 __declspec(thread,align(64))


typedef unsigned __int64 u64;
typedef unsigned __int32 u32;
typedef unsigned __int16 u16;
typedef unsigned __int8  u8;
typedef unsigned char    byte;

typedef signed __int64 i64;
typedef signed __int32 i32;
typedef signed __int16 i16;
typedef signed __int8  i8;

#else 

#include <stdbool.h>
#include <inttypes.h>
#include <stdio.h>

#include <atomic_ops.h>
#include "atomic_ops_if.h"


#define _mm_pause()     __asm__("pause;")


#define CompareSwap32(ptr32,cmp32,val32)      ATOMIC_CAS_MB32(ptr32,cmp32,val32)


typedef  uint64_t    u64;
typedef  uint32_t      u32;
typedef  uint16_t      u16;
typedef  uint8_t       u8;
typedef  char        byte;

typedef  int64_t    i64;
typedef  int32_t      i32;
typedef  int16_t      i16;
typedef  int8_t       i8;


#endif

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define CACHE_LINE    128

#ifdef _MSC_VER

//#define CACHE_ALIGN   __declspec(align(CACHE_LINE))
#define CACHE_ALIGN   __declspec(align(64))

#else

#define CACHE_ALIGN   __attribute__((aligned(64)))

#endif

/* From 48-proc machine
#define MAX_THREADS  48

#define NUM_NODES                 8
#define NUM_SOCKETS               4
#define NUM_THREADS_PER_NODE      6
#define NUM_NODES_PER_SOCKET      2
#define NUM_THREADS_PER_SOCKET    12   // NUM_NODES_PER_SOCKET * NUM_THREADS_PER_NODE
#define MAX_COMBINE       6
*/

#define MAX_THREADS  80

#define NUM_NODES                 4
#define NUM_SOCKETS               4
#define NUM_THREADS_PER_NODE      20
#define NUM_NODES_PER_SOCKET      1
#define NUM_THREADS_PER_SOCKET    20   // NUM_NODES_PER_SOCKET * NUM_THREADS_PER_NODE
#define MAX_COMBINE				  NUM_THREADS_PER_NODE


#define NUM_THR_NODE              NUM_THREADS_PER_NODE

#define INIT_EXPB    128

_inline void Backoff(u32 times);

struct PaddedUIntS {
	u32 val;
	char pad_[CACHE_LINE - sizeof(u32)];
} CACHE_ALIGN;

struct PaddedVolatileUIntS {
	volatile u32 val;
	char pad_[CACHE_LINE - sizeof(u32)];
} CACHE_ALIGN;

typedef struct PaddedUIntS   PaddedUInt;
typedef struct PaddedVolatileUIntS PaddedVolatileUInt;


/*
class Spinlock {
private:
	PaddedVolatileUInt spinvar;
public:
	Spinlock() {
		spinvar.val = 0;
	}

	void lock() {
		u32 expb = 1;
		do {
			while (spinvar.val != 0) {
				//Backoff(expb);
				Backoff(1);
				expb *= 2;
			}
		} while (CompareSwap32(&(spinvar.val), 0, 1) != 0);
	}

	void unlock() {
		spinvar.val = 0;
	}
} CACHE_ALIGN;
*/


struct NodeRWLock_DistS {
	PaddedVolatileUInt rLock[NUM_THR_NODE];
} CACHE_ALIGN;

typedef struct NodeRWLock_DistS  NodeRWLock_Dist;

inline void NodeRWLock_Dist_Init(NodeRWLock_Dist *lk);
inline bool NodeRWLock_Dist_TryRLock(NodeRWLock_Dist *lk, u32 id);
inline void NodeRWLock_Dist_RLock(NodeRWLock_Dist *lk, u32 id);
inline void NodeRWLock_Dist_RUnlock(NodeRWLock_Dist *lk, u32 id);
inline bool NodeRWLock_Dist_TryWLock(NodeRWLock_Dist *lk, u32 id);
inline void NodeRWLock_Dist_WLock(NodeRWLock_Dist *lk);
inline void NodeRWLock_Dist_WUnlock(NodeRWLock_Dist *lk);


inline void Backoff(u32 times) {
	u32 t;
	u32 max = times;
	if (max < INIT_EXPB) max = INIT_EXPB;
	for (t = 0; t < max; ++t) {
		_mm_pause();
	}
}

inline void NodeRWLock_Dist_Init(NodeRWLock_Dist *lk) {
	int i;

	//printf("\n-----------------> RWLOCKINIT %p\n", lk);

	for (i = 0; i < NUM_THR_NODE; ++i) lk->rLock[i].val = 0;
}


inline bool NodeRWLock_Dist_TryRLock(NodeRWLock_Dist *lk, u32 id) {
	if (lk->rLock[id].val != 0) return false;
	if (CompareSwap32(&(lk->rLock[id].val), 0, 1) == 0) {
		return true;
	}
	return false;
}

inline void NodeRWLock_Dist_RLock(NodeRWLock_Dist *lk, u32 id) {
	u32 v;
	do {
		while (lk->rLock[id].val != 0) Backoff(1024);
		if ((v = CompareSwap32(&(lk->rLock[id].val), 0, 1)) == 0) {
			return;
		}
	} while (1);
}

inline void NodeRWLock_Dist_RUnlock(NodeRWLock_Dist *lk, u32 id) {
	lk->rLock[id].val = 0;
}

inline bool NodeRWLock_Dist_TryWLock(NodeRWLock_Dist *lk, u32 id) {
	int i;
	if (lk->rLock[id].val != 0) return false;
	if (!NodeRWLock_Dist_TryRLock(lk, 0)) return false;
	for (i = 1; i < NUM_THR_NODE; ++i) {
		NodeRWLock_Dist_RLock(lk, i);
	}
	return true;
}

inline void NodeRWLock_Dist_WLock(NodeRWLock_Dist *lk) {
	int i;

	for (i = 0; i < NUM_THR_NODE; ++i) {
		NodeRWLock_Dist_RLock(lk, i);
	}
}


inline void NodeRWLock_Dist_WUnlock(NodeRWLock_Dist *lk) {
	int i;
	for (i = 0; i < NUM_THR_NODE; ++i) lk->rLock[i].val = 0;
}



////////////////////////////////////////

	/*
class NodeRWLock_Dist {
private:
	PaddedVolatileUInt rLock[NUM_THR_NODE];	
public:

	NodeRWLock_Dist() {
		for (int i = 0; i < NUM_THR_NODE; ++i) rLock[i].val = 0;
	}


	inline bool TryRLock(u32 id) {
		if (rLock[id].val != 0) return false;
		if (CompareSwap32(&(rLock[id].val), 0, 1) == 0) {
			return true;
		}
		return false;
	}

	inline void RLock(u32 id) {		
		do {
			while (rLock[id].val != 0) Backoff(1024);			
			if (CompareSwap32(&(rLock[id].val), 0, 1) == 0) {
				return;
			}
		} while (1);
	}

	inline void RUnlock(u32 id) {
		rLock[id].val = 0;
	}

	inline bool TryWLock(u32 id) {
		if (rLock[id].val != 0) return false;
		if (!TryRLock(0)) return false;
		for (int i = 1; i < NUM_THR_NODE; ++i) {
			RLock(i);
		}
		return true;
	}

	inline void WLock() {
		for (int i = 0; i < NUM_THR_NODE; ++i) {
			RLock(i);
		}
	}


	inline void WUnlock() {
		for (int i = 0; i < NUM_THR_NODE; ++i) rLock[i].val = 0;
	}

} CACHE_ALIGN;
*/

/*
class NodeRWLock {
private:
	PaddedVolatileUInt rCounter;
	PaddedVolatileUInt wLock;
public:

	NodeRWLock() {
		rCounter.val = 0;
		wLock.val = 0;
	}

	void RLock(u32 id) {
		u32  readers;		
		do {
			while (wLock.val != 0) _mm_pause();
			readers = rCounter.val;
			if (CompareSwap32(&(rCounter.val), readers, readers + 1) == readers) {
				if (wLock.val != 0) RUnlock(id);
				else return;
			}
		} while (1);
	}

	void RUnlock(u32 id) {
		u32 readers;
		do {
			readers = rCounter.val;
			if (CompareSwap32(&(rCounter.val), readers, readers - 1) == readers) {
				return;
			}
		} while (1);
	}

	void WLock() {
		do {			
			while (wLock.val != 0) _mm_pause();
			if (CompareSwap32(&(wLock.val), 0, 1) == 0) {
				while (rCounter.val > 0);
				return;
			}
		} while (1);
	}


	void WUnlock() {
		wLock.val = 0;
	}

} CACHE_ALIGN;
*/


#endif // _UTILITY_H