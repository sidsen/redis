#ifndef _FLAT_COMBINING_H
#define _FLAT_COMBINING_H

#include "utility.h"
#include "..\dict.h"

#if defined (METHOD_FLAT_COMBINING)

struct FCS;
typedef struct FCS   FC;


//TODO:RDS TEMPORARILY CREATE GLOBAL INSTANCE
extern FC* fc;
extern u32 threadCounter;
extern dict* thread_ids;
/* Hash type hash table (note that small hashes are represented with ziplists) */
extern dictType IntDictType;


#define SharedDSType		void

#define sl_contains(s, k)      FC_contains(s, ID, k, 0)
#define sl_add(s, k, v)        FC_insert(s, ID, k, v)
#define sl_remove(s, k)        FC_remove(s, ID, k, 0)
#define sl_set_size(s)         FC_size(s)
#define sl_set_new()           FC_new()


struct FCS {	
	SharedDSType *localReg;
	char pad_[CACHE_LINE - sizeof(SharedDSType*)];
	PaddedVolatileUInt combinerLock;
	PaddedSlot slot[MAX_THREADS];
} CACHE_ALIGN;


FC* FC_new();
void FC_StartThread(FC *rds, int thrid);
u32 FC_contains(FC *rds, int thrid, u32 arg1, u32 arg2);
u32 FC_insert(FC *rds, int thrid, u32 arg1, u32 arg2);
u32 FC_incrby(FC *rds, int thrid, u32 arg1, u32 arg2);
u32 FC_remove(FC *rds, int thrid, u32 arg1, u32 arg2);

#endif  

#endif