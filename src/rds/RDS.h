#ifndef _SHAREDDS_H
#define _SHAREDDS_H

//#include "utility.h"
#include "SharedLog.h"
#include "..\dict.h"

struct RDSS;
typedef struct RDSS   RDS;


//TODO:RDS TEMPORARILY CREATE GLOBAL INSTANCE
extern RDS* rds;
extern u32 threadCounter;
extern dict* thread_ids;
/* Hash type hash table (note that small hashes are represented with ziplists) */
extern dictType IntDictType;


// This is using the skiplist from ASCYLIB
//#include "seq.h"
//#define sl_intset_t       RDS
//#define SharedDSType      sl_intset_t
#define SharedDSType		void

#define sl_contains(s, k)      RDS_contains(s, ID, k, 0)
#define sl_add(s, k, v)        RDS_insert(s, ID, k, v)
#define sl_remove(s, k)        RDS_remove(s, ID, k, 0)
#define sl_set_size(s)         RDS_size(s)
#define sl_set_new()           RDS_new()



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
	volatile u32 op;
	u32 arg1;
	u32 arg2;
	volatile u32* resp;
	char pad_[CACHE_LINE - sizeof(volatile u32)-2 * sizeof(u32)-sizeof(volatile u32*)];
} CACHE_ALIGN;

typedef struct PaddedSlotS PaddedSlot;

struct NodeReplica_OPTRS {
	SharedDSType *localReg;
	u32 localTail;
	u32 localBit;
	u32 startId;
	u32 endId;
	char pad_[CACHE_LINE - 4 * sizeof(u32)-sizeof(SharedDSType*)];
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
	SharedLog sharedLog;
	PaddedNodeReplicaPtr_OPTR local[MAX_THREADS];
	PaddedUInt leader[MAX_THREADS];
};


RDS* RDS_new();
//TODO:SID NOT NEEDED?
//int RDS_size(RDS *rds);
void RDS_StartThread(RDS *rds, int thrid);
u32 RDS_contains(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_insert(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_incrby(RDS *rds, int thrid, u32 arg1, u32 arg2);
u32 RDS_remove(RDS *rds, int thrid, u32 arg1, u32 arg2);

#endif  // _SHAREDDS_H