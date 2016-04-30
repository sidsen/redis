#ifndef _FLAT_COMBINING_H
#define _FLAT_COMBINING_H

#include "utility.h"
#include "..\dict.h"

struct FCS;
typedef struct FCS FC;

//TODO:FC TEMPORARILY CREATE GLOBAL INSTANCE
extern volatile FC*  fc;
extern dict* thread_ids_fc;
/* Hash type hash table (note that small hashes are represented with ziplists) */
#ifndef IntDictType
extern dictType IntDictType;
#endif


#define SharedDSType		void

/*
#define sl_contains(s, k)      FC_contains(s, ID, k, 0)
#define sl_add(s, k, v)        FC_insert(s, ID, k, v)
#define sl_remove(s, k)        FC_remove(s, ID, k, 0)
#define sl_set_size(s)         FC_size(s)
#define sl_set_new()           FC_new()
*/

struct FCS {	
	SharedDSType *localReg;
	char pad_[CACHE_LINE - sizeof(SharedDSType*)];
	volatile u32 threadCnt;
	PaddedVolatileUInt combinerLock;
#ifdef FCRW	
	NodeRWLock_Dist            rwlock;		
#endif

#ifdef RWL	
	NodeRWLock_Dist            rwlock;
#endif
	PaddedSlot slot[MAX_THREADS];
} CACHE_ALIGN;


FC* FC_new();
void FC_StartThread(FC *fc, int thrid);
u32 FC_contains(FC *fc, int thrid, u32 arg1, u32 arg2);
u32 FC_insert(FC *fc, int thrid, u32 arg1, u32 arg2);
u32 FC_incrby(FC *fc, int thrid, u32 arg1, u32 arg2);
u32 FC_remove(FC *fc, int thrid, u32 arg1, u32 arg2);

#endif