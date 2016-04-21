
#include "fc.h"
//FLAT COMBINING
#include "..\redis.h"

//TODO:FC TEMPORARILY CREATE GLOBAL 
FC* fc;
dict* thread_ids_fc;

/* Hash type hash table (note that small hashes are represented with ziplists) */
#ifndef IntDictType
dictType IntDictType = {
	dictIntHashFunction,             /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	NULL,						/* key compare */
	NULL,						/* key destructor */
	NULL						/* val destructor */
};
#endif

/**********************************************************
**                    LOCAL
***********************************************************/


inline u32 FC_contains_local(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return zrankGenericCommandLocal(fc->localReg, arg1);
}

inline u32 FC_insert_local(FC *fc, int thrid, u32 arg1, u32 arg2) {	
	return zaddGenericCommandLocal(fc->localReg, arg1, arg2, 0);
}

inline u32 FC_remove_local(FC *fc, int thrid, u32 arg1, u32 arg2) {
	redisAssert(0);
	return 0; 
}

inline u32 FC_incrby_local(FC *fc, int thrid, u32 arg1, u32 arg2) {	
	return zaddGenericCommandLocal(fc->localReg, arg1, arg2, 1);
}

inline u32 Execute_local_fc(FC *fc, int thrid, u32 op, u32 arg1, u32 arg2) {
	switch (op) {
	case CONTAINS:
		return FC_contains_local(fc, thrid, arg1, arg2);
	case INSERT:
		return FC_insert_local(fc, thrid, arg1, arg2);
	case REMOVE:
		return FC_remove_local(fc, thrid, arg1, arg2);
	case INCRBY:
		return FC_incrby_local(fc, thrid, arg1, arg2);
	default:
		redisAssert(0);
	}
}


/**********************************************************
**                    PRIVATE DS FUNCTIONALITY
***********************************************************/

#if 0
inline void DoOp_fc(FC *fc, u32 thrid, u32 op, u32 arg1, u32 arg2) {
	switch (op & CYCLE_MASK) {	
	case INSERT:
		FC_insert_local(fc, thrid, arg1, arg2);
		break;
	case REMOVE:
		FC_remove_local(fc, thrid, arg1, arg2);
		break;
	case INCRBY:
		FC_incrby_local(fc, thrid, arg1, arg2);
		break;
	case CONTAINS:
		redisAssert(0);
		// ignore read-only operations in the log							
		break;
	case EMPTY:
		redisAssert(0);
		break;
	default:
		redisAssert(0);
		break;
	}
}
#endif

u32 Combine_fc(FC *fc, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;	
	u32 index;
	u32 myIndex = thrid;

	fc->slot[myIndex].resp.val = MAX_UINT32;
	fc->slot[myIndex].arg1 = arg1;
	fc->slot[myIndex].arg2 = arg2;	
	fc->slot[myIndex].op = op;

	int NUM_RET = MAX_THREADS;

	do {

		if ((fc->combinerLock.val == 0) 
			&& (fc->combinerLock.val == 0)
			&& (fc->combinerLock.val == 0)
			&& (CompareSwap32(&(fc->combinerLock.val), 0, 1) == 0)) {
			// I am the Combiner		

			for (int retries = 0; retries < NUM_RET; ++retries) {

				for (u32 index = 0; (index < MAX_THREADS); ++index) {
					nextOp = fc->slot[index].op & CYCLE_MASK;
					fc->slot[index].op = EMPTY;
					fc->slot[index].resp.val = Execute_local_fc(fc, thrid, nextOp, fc->slot[index].arg1, fc->slot[index].arg2);
				}
			}

			fc->combinerLock.val = 0;

			redisAssert(fc->slot[myIndex].resp.val != MAX_UINT32);
			return fc->slot[myIndex].resp.val;
		}
		else {
			// not combiner, wait for response or wait to become combiner

			while ((fc->slot[myIndex].resp.val == MAX_UINT32) 
				&&  (fc->combinerLock.val != 0)) {
				_mm_pause();
			}

			if (fc->slot[myIndex].resp.val != MAX_UINT32) {
				return fc->slot[myIndex].resp.val;
			}
		}			
			
	} while (1);
}

#if 0
inline void NodeReplica_OPTR_Init_fc(NodeReplica_OPTR *nr) {
	int i;
	//printf("\n-----------------> NodeReplica_init %d\n", 0);
	//nr->localReg = (SharedDSType*)malloc(sizeof(SharedDSType));
	nr->localReg = createZsetObject();  // sl_set_new_local();
	nr->localTail = 0;
	nr->localBit = 1;
	nr->combinerLock.val = 0;
	nr->startId = nr->endId = 0;
	NodeRWLock_Dist_Init(&(nr->lock));
	for (i = 0; i < NUM_THREADS_PER_NODE; ++i) nr->slot[i].op = EMPTY;
}
#endif


/**********************************************************
**                    API
***********************************************************/


void FC_Start(FC *fc) {	
	int i;
	fc->localReg = createZsetObject();  // sl_set_new_local();
	fc->combinerLock.val = 0;
	for (i = 0; i < MAX_THREADS; ++i) fc->slot[i].op = EMPTY;
	//maxFCThreads = 0;
}

FC* FC_new() {
	FC *fc = (FC *)malloc(sizeof(FC));	
	FC_Start(fc);
	thread_ids_fc = dictCreate(&IntDictType, NULL);
	dictExpand(thread_ids_fc, MAX_THREADS);
	return fc;
}

void FC_StartThread(FC *fc, int thrid) {
	dictAdd(thread_ids_fc, GetCurrentThreadId(), thrid);
	//FetchAndAdd32(&maxFCThreads, 1);
}

u32 FC_contains(FC *fc, int thrid, u32 arg1, u32 arg2) {	
	return Combine_fc(fc, thrid, CONTAINS, arg1, arg2);
}

u32 FC_insert(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine_fc(fc, thrid, INSERT, arg1, arg2);		
}

u32 FC_incrby(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine_fc(fc, thrid, INCRBY, arg1, arg2);
}

u32 FC_remove(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine_fc(fc, thrid, REMOVE, arg1, arg2);
}

void FC_FinishThread(FC *fc, int thrid) {
}

void FC_Finish(FC *fc) {
}
