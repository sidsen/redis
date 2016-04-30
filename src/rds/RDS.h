#ifndef _REPLICATED_DS_H
#define _REPLICATED_DS_H

#include "utility.h"
#include "SharedLog.h"
#include "..\dict.h"

//#if defined (METHOD_REPLICATION)

struct RDSS;
typedef struct RDSS RDS;

//TODO:RDS TEMPORARILY CREATE GLOBAL INSTANCE
extern RDS* rds;
extern dict* thread_ids;
/* Hash type hash table (note that small hashes are represented with ziplists) */
#if not defined(IntDictType)
extern dictType IntDictType;
#endif


// This is using the skiplist from ASCYLIB
//#include "seq.h"
//#define sl_intset_t       RDS
//#define SharedDSType      sl_intset_t
#define SharedDSType		void

/*
#define sl_contains(s, k)      RDS_contains(s, ID, k, 0)
#define sl_add(s, k, v)        RDS_insert(s, ID, k, v)
#define sl_remove(s, k)        RDS_remove(s, ID, k, 0)
#define sl_set_size(s)         RDS_size(s)
#define sl_set_new()           RDS_new()
*/

struct NodeReplica_OPTRS {
	SharedDSType *localReg;
	u32 localTail;
	u32 localBit;
	// Per replica thread count
	u32 threadCnt;
	char pad_[CACHE_LINE - 3 * sizeof(u32)-sizeof(SharedDSType*)];
	PaddedVolatileUInt combinerLock;	
	NodeRWLock_Dist lock;
	PaddedSlot slot[NUM_THREADS_PER_NODE];
} CACHE_ALIGN;

typedef struct NodeReplica_OPTRS                        NodeReplica_OPTR;

	

union PaddedNodeReplicaPtr_OPTRU {
	NodeReplica_OPTR * replica;
	char pad_[CACHE_LINE];
} CACHE_ALIGN;


typedef union PaddedNodeReplicaPtr_OPTRU      PaddedNodeReplicaPtr_OPTR;

// Node replica with optimized reads, using NodeRWLock_Dist
// (distributed reader-writer lock) with fallback to Combine.
// First try to acquire Reader lock, if possible, then check if the thread can read locally. 
// If it can't acquire the lock, or if it can't read, then the thread tries to combine. 
// Register_NR3_OPTR {
struct RDSS {
	// Global thread count (note: volatile seems essential)
	//volatile u32 threadCnt;
	PaddedVolatileUInt threadCnt;
	SharedLog sharedLog;
	PaddedNodeReplicaPtr_OPTR local[MAX_THREADS];
	PaddedUInt leader[MAX_THREADS];
};


RDS* RDS_new();
//TODO:SID NOT NEEDED?
//int RDS_size(RDS *rds);
void RDS_StartThread(RDS *rds, int thrid);
void RDS_SetReplicaThreadCounts(RDS *rds);
u32 RDS_contains(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_insert(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_incrby(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_remove(RDS *rds, int thrid, u32 arg1, u32 arg2);

//#endif 

#endif