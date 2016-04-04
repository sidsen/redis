
#include "fc.h"
//FLAT COMBINING
#include "..\redis.h"


#if defined (METHOD_FLAT_COMBINING)


//TODO:RDS TEMPORARILY CREATE GLOBAL 
FC* fc;
u32 threadCounter = 0;
dict* thread_ids;

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType IntDictType = {
	dictIntHashFunction,             /* hash function */
	NULL,                       /* key dup */
	NULL,                       /* val dup */
	NULL,						/* key compare */
	NULL,						/* key destructor */
	NULL						/* val destructor */
};

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

inline u32 Execute_local(FC *fc, int thrid, u32 op, u32 arg1, u32 arg2) {
	switch (op) {
	case CONTAINS:
		return RDS_contains_local(fc, thrid, arg1, arg2);
	case INSERT:
		return RDS_insert_local(fc, thrid, arg1, arg2);
	case REMOVE:
		return RDS_remove_local(fc, thrid, arg1, arg2);
	case INCRBY:
		return RDS_incrby_local(fc, thrid, arg1, arg2);
	default:
		redisAssert(0);
	}
}


/**********************************************************
**                    PRIVATE DS FUNCTIONALITY
***********************************************************/


inline void DoOp(FC *fc, u32 thrid, u32 op, u32 arg1, u32 arg2) {
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


u32 Combine(FC *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;	
	u32 index;
	u32 myIndex = thrid % NUM_THREADS_PER_NODE;

	rds->slot[myIndex].resp.val = MAX_UINT32;
	rds->slot[myIndex].arg1 = arg1;
	rds->slot[myIndex].arg2 = arg2;	
	rds->slot[myIndex].op = op;

	do {

		if ((rds->combinerLock.val == 0) 
			&& (rds->combinerLock.val == 0)
			&& (rds->combinerLock.val == 0)
			&& (CompareSwap32(&(rds->combinerLock.val), 0, 1) == 0)) {
			// I am the Combiner		

			for (index = 0; (index < NUM_THREADS_PER_NODE); ++index) {		
				nextOp = rds->slot[index].op  & CYCLE_MASK;
				rds->slot[index].op = EMPTY;
				rds->slot[index].resp.val = Execute_local(rds, thrid, nextOp, rds->slot[index].arg1, rds->slot[index].arg2);
			}

			rds->combinerLock.val = 0;

			redisAssert(rds->slot[myIndex].resp.val != MAX_UINT32);
			return rds->slot[myIndex].resp.val;
		}
		else {
			// not combiner, wait for response or wait to become combiner

			while ((rds->slot[myIndex].resp.val == MAX_UINT32) 
				&&  (rds->combinerLock.val != 0)) {
				_mm_pause();
			}

			if (rds->slot[myIndex].resp.val != MAX_UINT32) {
				return rds->slot[myIndex].resp.val;
			}
		}			
			
	} while (1);
}


inline void NodeReplica_OPTR_Init(NodeReplica_OPTR *nr) {
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



/**********************************************************
**                    API
***********************************************************/


void FC_Start(FC *fc) {	
}

FC* FC_new() {
	FC *fc = (FC *)malloc(sizeof(FC));	
	FC_Start(fc);
	thread_ids = dictCreate(&IntDictType, NULL);
	dictExpand(thread_ids, MAX_THREADS);
	return fc;
}

void FC_StartThread(RDS *rds, int thrid) {
	dictAdd(thread_ids, GetCurrentThreadId(), thrid);
}

u32 FC_contains(FC *fc, int thrid, u32 arg1, u32 arg2) {	
	return Combine(fc, thrid, CONTAINS, arg1, arg2);
}

u32 FC_insert(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine(fc, thrid, INSERT, arg1, arg2);		
}

u32 FC_incrby(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine(fc, thrid, INCRBY, arg1, arg2);
}

u32 FC_remove(FC *fc, int thrid, u32 arg1, u32 arg2) {
	return Combine(fc, thrid, REMOVE, arg1, arg2);
}

void FC_FinishThread(FC *fc, int thrid) {
}

void FC_Finish(FC *fc) {
}



#endif