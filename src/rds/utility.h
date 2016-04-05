//
// utility.h : Common utility classes and definitions.
//

#ifndef _UTILITY_H
#define _UTILITY_H

//#ifdef _MSC_VER
//#pragma once
//#define _CRT_SECURE_NO_WARNINGS
//#pragma warning(disable: 4996)
//#endif

#ifdef _WIN32
#if (!defined (__cplusplus) && (!defined (inline)))
#define inline __inline
#endif
#include <stdbool.h>
#include <stddef.h>
#endif

#include <assert.h>
#include <intrin.h>

#ifdef _MSC_VER


#define METHOD_REPLICATION
//#define METHOD_FLAT_COMBINING

/*******************************************************
CONFIGURATION
********************************************************/

/* if this is enabled all memory is allocated from node 0
otherwise memory is allocated from the node local to each processor */
#if 0
#define TEST_MEM_LOCALITY
#endif

/* pick machine */
#define MACHINE_RAMA
//#define MACHINE_VRG


/**********************************************
Change between:
- combine within a node
- combine 2 (or more?) nodes
- combine fewer than all threads in a node
************************************************/

#define NR_COMBINE_NODE
//#define NR_COMBINE_DOUBLE_NODE
//#define NR_COMBINE_SPLIT_NODE


/*******************************************************
END OF CONFIGURATION
********************************************************/



/*******************************************************
MACHINE-SPECIFIC DEFINES
********************************************************/


#define CACHE_LINE    128
//#define CACHE_ALIGN   __declspec(align(CACHE_LINE))
#define CACHE_ALIGN   __declspec(align(CACHE_LINE))


/* RAMA - MSR machine */
#ifdef MACHINE_RAMA

#ifdef NR_COMBINE_NODE
#define NUM_THREADS_PER_NODE      6
#elif defined NR_COMBINE_DOUBLE_NODE
#define NUM_THREADS_PER_NODE      12
#elif defined NR_COMBINE_SPLIT_NODE
#define NUM_THREADS_PER_NODE      3
#else
#error "NUM_THREADS_PER_NODE not defined"
#endif

//#define NUM_NODES                 8
//#define NUM_SOCKETS               4
//#define NUM_NODES_PER_SOCKET      2
//#define NUM_THREADS_PER_SOCKET    12   // NUM_NODES_PER_SOCKET * NUM_THREADS_PER_NODE


// within a node first; (core, hyperthread) pairs
static int coresInNodeAll[] = {
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
	14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
	28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48
};

#define INIT_EXPB    128

// for RWlock
#define NUM_THR_NODE    NUM_THREADS_PER_NODE

#define MAX_THREADS    48
//#if MAX_THREADS < (NUM_NODES * NUM_THREADS_PER_NODE)
//	#error "MAX_THREADS is invalid (must be bigger than NUM_NODES * NUM_THREADS_PER_NODE)"
//#endif

/* VRG-11 */
#elif defined MACHINE_VRG

#ifdef NR_COMBINE_NODE
#define NUM_THREADS_PER_NODE      28
#elif defined NR_COMBINE_DOUBLE_NODE
#define NUM_THREADS_PER_NODE      56
#elif defined NR_COMBINE_SPLIT_NODE
#define NUM_THREADS_PER_NODE      14
#else
#error "NUM_THREADS_PER_NODE not defined"
#endif

//#define NUM_NODES                 4
//#define NUM_SOCKETS               4
//#define NUM_NODES_PER_SOCKET      1
//#define NUM_THREADS_PER_SOCKET    28   // NUM_NODES_PER_SOCKET * NUM_THREADS_PER_NODE




#define INIT_EXPB    128

// for RWlock
#define NUM_THR_NODE    NUM_THREADS_PER_NODE


#define MAX_THREADS    128	
//#if MAX_THREADS < (NUM_NODES * NUM_THREADS_PER_NODE)
//	#error "MAX_THREADS is invalid (must be bigger than NUM_NODES * NUM_THREADS_PER_NODE)"
//#endif

// within a node first; (core, hyperthread) pairs
static int coresInNodeAll[] = {
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
	14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
	28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
	56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
	70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83,
	84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111
};

// within a node first; all cores first; then all hyperthreads in the same node
static int coresInNodeHtt[] = {
	0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26,
	1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27,
	28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54,
	29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55,
	56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82,
	57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83,
	84, 86, 88, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110,
	85, 87, 89, 91, 93, 95, 97, 99, 101, 103, 105, 107, 109, 111
};

// within a node first, only the cores; then cores from other nodes; then all the hyperthreads
static int coresInNodeNoHtt[] = {
	0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26,
	28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54,
	56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82,
	84, 86, 88, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110,
	1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27,
	29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55,
	57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83,
	85, 87, 89, 91, 93, 95, 97, 99, 101, 103, 105, 107, 109, 111
};

// across nodes, use hyperthreads too
static int coresOutNodeHtt[] = {
	0, 28, 56, 84, 1, 29, 57, 85,
	2, 30, 58, 86, 3, 31, 59, 87,
	4, 32, 60, 88, 5, 33, 61, 89,
	6, 34, 62, 90, 7, 35, 63, 91,
	8, 36, 64, 92, 9, 37, 65, 93,
	10, 38, 66, 94, 11, 39, 67, 95,
	12, 40, 68, 96, 13, 41, 69, 97,
	14, 42, 70, 98, 15, 43, 71, 99,
	16, 44, 72, 100, 17, 45, 73, 101,
	18, 46, 74, 102, 19, 47, 75, 103,
	20, 48, 76, 104, 21, 49, 77, 105,
	22, 50, 78, 106, 23, 51, 79, 107,
	24, 52, 80, 108, 25, 53, 81, 109,
	26, 54, 82, 110, 27, 55, 83, 111
};

// across nodes, don't use hyperthreads until we run out of cores
static int coresOutNodeNoHtt[] = {
	0, 28, 56, 84, 2, 30, 58, 86,
	4, 32, 60, 88, 6, 34, 62, 90,
	8, 36, 64, 92, 10, 38, 66, 94,
	12, 40, 68, 96, 14, 42, 70, 98,
	16, 44, 72, 100, 18, 46, 74, 102,
	20, 48, 76, 104, 22, 50, 78, 106,
	24, 52, 80, 108, 26, 54, 82, 110,
	1, 29, 57, 85, 3, 31, 59, 87,
	5, 33, 61, 89, 7, 35, 63, 91,
	9, 37, 65, 93, 11, 39, 67, 95,
	13, 41, 69, 97, 15, 43, 71, 99,
	17, 45, 73, 101, 19, 47, 75, 103,
	21, 49, 77, 105, 23, 51, 79, 107,
	25, 53, 81, 109, 27, 55, 83, 111,
};

#else 
#error "Unrecognized machine, set a machine in utility.h"
#endif 


/*******************************************************
ENDOF MACHINE-SPECIFIC DEFINES
********************************************************/



#define MEMBAR  _ReadWriteBarrier()


// these definitions for fetch-and-add, atomic-increment, and atomic-decrement are compiler specific
#define FetchAndAdd32(ptr32, val32) _InterlockedExchangeAdd((long*)ptr32,val32)
#define FetchAndAdd64(ptr64, val64) _InterlockedExchangeAdd64((long*)ptr64,val64)
#define FetchSwapPtr(ptr, val) _InterlockedExchangePointer((void* volatile*)ptr,val)
#define AtomicInc32(ptr32) _InterlockedIncrement((long*)ptr32)
#define AtomicInc64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define AtomicDec32(ptr32) _InterlockedDecrement((long*)ptr32)
#define AtomicDec64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define CompareSwap32(ptr32,cmp32,val32) _InterlockedCompareExchange((long*)ptr32,val32,cmp32)
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

// Thread pinning in Windows
void pinThread(u32 proc);

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


#define CYCLE_BITS   31
#define CYCLE_MASK   2147483647   // (1 << CYCLE_BITS) - 1
#define OP_BITS      2            // op only uses the last 2 bits    
#define OP_MASK      3            // keep op bits discard everything else
#define NODE_MASK    28           // node mask discards the op bits and uses the next 3 bits for the node
#define MAX_UINT32   4294967295   // (1 << 32) - 1


enum {
	EMPTY,
	CONTAINS,
	INSERT,
	REMOVE,
	INCRBY
};


struct PaddedSlotS {
	PaddedVolatileUInt resp;
	u32 arg1;
	u32 arg2;
	volatile u32 op;
	char pad_[CACHE_LINE - sizeof(volatile u32)-2 * sizeof(u32)];
} CACHE_ALIGN;

typedef struct PaddedSlotS PaddedSlot;

// Spinlock 
struct SpinLockS {
	PaddedVolatileUInt spinvar;
} CACHE_ALIGN;

typedef struct SpinLockS SpinLock;

inline void SpinLock_Init(SpinLock *lk) {
	lk->spinvar.val = 0;
}

inline void SpinLock_Destroy(SpinLock *lk) {
	lk->spinvar.val = MAX_UINT32;
}

inline void SpinLock_Lock(SpinLock *lk) {
	u32 expb = 1;
	do {
		while (lk->spinvar.val != 0) {
			//Backoff(expb);
			Backoff(1);
			expb *= 2;
		}
	} while (CompareSwap32(&(lk->spinvar.val), 0, 1) != 0);
}

inline bool SpinLock_TryLock(SpinLock *lk) {
	return (CompareSwap32(&(lk->spinvar.val), 0, 1) == 0);
}

inline void SpinLock_Unlock(SpinLock *lk) {
	lk->spinvar.val = 0;
}

inline void Backoff(u32 times) {
	u32 t;
	u32 max = times;
	if (max < INIT_EXPB) max = INIT_EXPB;
	for (t = 0; t < max; ++t) {
		_mm_pause();
	}
}

// New RW Lock
struct NodeRWLock_DistS {
	PaddedVolatileUInt wLock;
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

inline void NodeRWLock_Dist_Init(NodeRWLock_Dist *lk) {
	int i;

	//printf("\n-----------------> RWLOCKINIT %p\n", lk);

	lk->wLock.val = 0;
	for (i = 0; i < NUM_THR_NODE; ++i) lk->rLock[i].val = 0;
}

inline bool NodeRWLock_Dist_TryRLock(NodeRWLock_Dist *lk, u32 id) {
	if (lk->wLock.val == 0) {
		lk->rLock[id].val = 1;
		MEMBAR;
		if (lk->wLock.val == 0) return true;
		else {
			lk->rLock[id].val = 0;
			MEMBAR;
			return false;
		}
	}
	return false;
}

inline void NodeRWLock_Dist_RLock(NodeRWLock_Dist *lk, u32 id) {
	do {
		if (lk->wLock.val == 0) {
			lk->rLock[id].val = 1;
			MEMBAR;
			if (lk->wLock.val == 0) return;
			else {
				lk->rLock[id].val = 0;
				MEMBAR;
			}
		}
		_mm_pause();
	} while (1);
}

inline void NodeRWLock_Dist_RUnlock(NodeRWLock_Dist *lk, u32 id) {
	lk->rLock[id].val = 0;
	MEMBAR;
}


inline void NodeRWLock_Dist_WLock(NodeRWLock_Dist *lk) {
	int i;
	// we always have only one writer, so we don't need to synchronize on the writer lock	
	assert(lk->wLock.val == 0);
	lk->wLock.val = 1;
	MEMBAR;
	// wait for the readers
	for (i = 0; i < NUM_THR_NODE; ++i)  {
		while (lk->rLock[i].val > 0);
	}
}


inline void NodeRWLock_Dist_WUnlock(NodeRWLock_Dist *lk) {
	lk->wLock.val = 0;
	// combiner lock is always released after this lock
	//MEMBAR;
}


///
/// Thread-local pseudo-random number generator
///

u16 randLFSR();



/*
class SimplePrng { // Simple generator used to seed the better generator
private:
	unsigned long n;
public:
	SimplePrng(){ n = (unsigned long)time(0); }
	void SetSeed(long s){ n = s; }
	SimplePrng(int seed){ n = seed; }
	unsigned long next(void){
		n = n * 1103515245 + 12345;
		return (unsigned int)(n / 65536) % 32768;
	}
};

class Prng {
private:
	SimplePrng seeder;
	unsigned __int64 Y[55];
	int j, k;
public:
	Prng() : seeder() { init(); }
	Prng(unsigned long seed) : seeder(seed){ init(seed); }
	void init(unsigned long seed = 0){ // uses simpler generator to produce a seed
		if (seed != 0) seeder.SetSeed(seed);
		else seeder.SetSeed((unsigned long)time(0));
		unsigned __int64 v;
		for (int i = 0; i < 55; ++i){
			// generate 64-bit random number of 7-bit seeder
			v = 0;
			for (int h = 0; h < 10; ++h) v = (v << 7) | seeder.next();
			Y[i] = v;
		}
		j = 23;
		k = 54;
	}
	unsigned __int64 next(void){
		unsigned __int64 retval;
		Y[k] = Y[k] + Y[j];
		retval = Y[k];
		--j;
		if (j<0) j = 54;
		--k;
		if (k<0) k = 54;
		return retval;
	}
};
*/

#endif // _UTILITY_H