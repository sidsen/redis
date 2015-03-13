
#include "RDS.h"
//TODO:RDS
#include "..\redis.h"

//TODO:RDS TEMPORARILY CREATE GLOBAL 
RDS* rds;
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


inline u32 RDS_contains_local(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	return zrankGenericCommandLocal((rds->local[rds->leader[thrid].val].replica->localReg), arg1);
}

inline u32 RDS_insert_local(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> sl_add_local %d\n", 0);
	return zaddGenericCommandLocal((rds->local[rds->leader[thrid].val].replica->localReg), arg1, arg2, 0);
}

inline u32 RDS_remove_local(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	redisAssert(0);
	return 0; // sl_remove_local((rds->local[rds->leader[thrid].val].replica->localReg), arg1);
}

inline u32 RDS_incrby_local(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> sl_add_local %d\n", 0);
	return zaddGenericCommandLocal((rds->local[rds->leader[thrid].val].replica->localReg), arg1, arg2, 1);
}

inline u32 Execute_local(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	switch (op) {
	case CONTAINS:
		return RDS_contains_local(rds, thrid, arg1, arg2);
	case INSERT:
		return RDS_insert_local(rds, thrid, arg1, arg2);
	case REMOVE:
		return RDS_remove_local(rds, thrid, arg1, arg2);
	case INCRBY:
		return RDS_incrby_local(rds, thrid, arg1, arg2);
	default:
		redisAssert(0);
	}
}


/**********************************************************
**                    PRIVATE DS FUNCTIONALITY
***********************************************************/


inline void DoOp(RDS *rds, u32 thrid, u32 op, u32 arg1, u32 arg2) {
	switch (op & CYCLE_MASK) {
	case CONTAINS:
		// ignore read-only operations in the log							
		break;
	case INSERT:
		RDS_insert_local(rds, thrid, arg1, arg2);
		break;
	case REMOVE:
		RDS_remove_local(rds, thrid, arg1, arg2);
		break;
	case INCRBY:
		RDS_incrby_local(rds, thrid, arg1, arg2);
		break;
	case EMPTY:
		break;
	default:
		redisAssert(0);
		break;
	}
}


inline bool Between(RDS *rds, u32 min, u32 start, u32 howMany) {
	u32 end = ADDN(start, howMany);
	if (start < end) {
		return ((start <= min) && (min < end));
	}
	else {
		// start > end 
		return ((start <= min) || (min < end));
	}
}


void UpdateMin(RDS *rds, int thrid, u32 currentTail, u32 howMany) {
	u32 logMin, p;
	u32 auxTail;
	logMin = rds->sharedLog.logMin.val;
	p = (logMin + _logSize - MAX_COMBINE - 1) & (_logSize - 1);

	while (Between(rds, p, currentTail, howMany)) {
		//update logMin if possible
		int i;
		u32 min = _logSize;
		u32 minBiggerTail = _logSize;
		for (i = 0; i < MAX_THREADS; ++i) {
			if (rds->local[i].replica) {
				auxTail = rds->local[i].replica->localTail;
				if (auxTail < min) min = auxTail;
				if (auxTail >= rds->sharedLog.logMin.val && auxTail < minBiggerTail) minBiggerTail = auxTail;
			}
		}
		if (minBiggerTail == _logSize) {
			rds->sharedLog.logMin.val = min;
		}
		else {
			rds->sharedLog.logMin.val = minBiggerTail;
		}
		logMin = rds->sharedLog.logMin.val;
		p = (logMin + _logSize - MAX_COMBINE - 1) & (_logSize - 1);
	}
}

inline void UpdateFromLog(RDS *rds, int thrid, u32 to);

u32 AppendAndUpdate(RDS *rds, int thrid, u32 howMany) {
	u32 currentTail, to;


	if (howMany > 0) {
		do {
			currentTail = SharedLog_GetNextTail(&(rds->sharedLog), howMany);
			if (currentTail == _logSize) {
				to = rds->sharedLog.logTail.val;
				UpdateFromLog(rds, thrid, to);
				rds->local[rds->leader[thrid].val].replica->localTail = to;
			}
		} while (currentTail == _logSize);
	}
	else {
		currentTail = rds->sharedLog.logTail.val;
	}

	return currentTail;

}

inline void UpdateFromLog(RDS *rds, int thrid, u32 to) {
	u32 expb, index;
	if (rds->local[rds->leader[thrid].val].replica->localTail <= to) {
		for (index = rds->local[rds->leader[thrid].val].replica->localTail; index < to; ++index) {
			expb = INIT_EXPB;
			while (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				_mm_pause();
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
		}
	}
	else {
		for (index = rds->local[rds->leader[thrid].val].replica->localTail; index < _logSize; ++index) {
			expb = INIT_EXPB;
			while (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				_mm_pause();
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
		}

		rds->local[rds->leader[thrid].val].replica->localBit = 1 - rds->local[rds->leader[thrid].val].replica->localBit;

		for (index = 0; index < to; ++index) {
			expb = INIT_EXPB;
			while (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				_mm_pause();
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
		}
	}
}


inline u32 UpdateFromLogForReads(RDS *rds, int thrid, u32 to) {
	u32 index;
	u32 node = (thrid / NUM_THREADS_PER_NODE) << OP_BITS;
	if (rds->local[rds->leader[thrid].val].replica->localTail <= to) {
		for (index = rds->local[rds->leader[thrid].val].replica->localTail; index < to; ++index) {
			if (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				return index;
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
			// TODO why is OP_MASK here but CYCLE_MASK in UpdateFromLog. Does it matter which one we're using?
			/*
			switch (sharedLog.log[index].op & OP_MASK) {
			case RO:
			// ignore read-only operations in the log
			break;
			case WO:
			WriteOnly_local(thrid, sharedLog.log[index].arg1, sharedLog.log[index].arg2);
			break;
			case RW:
			ReadWrite_local(thrid, sharedLog.log[index].arg1, sharedLog.log[index].arg2);
			break;
			case EMPTY:
			break;
			default:
			assert(0);
			break;
			}
			*/
		}
	}
	else {
		for (index = rds->local[rds->leader[thrid].val].replica->localTail; index < _logSize; ++index) {
			if (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				return index;
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
		}

		rds->local[rds->leader[thrid].val].replica->localBit = 1 - rds->local[rds->leader[thrid].val].replica->localBit;

		for (index = 0; index < to; ++index) {
			if (((rds->sharedLog.log[index].op >> CYCLE_BITS) ^ rds->local[rds->leader[thrid].val].replica->localBit) != 0) {
				return index;
			}
			DoOp(rds, thrid, rds->sharedLog.log[index].op, rds->sharedLog.log[index].arg1, rds->sharedLog.log[index].arg2);
		}
	}

	return to;
}



u32 Combine(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;
	volatile u32 resp = MAX_UINT32;
	u32 myIndex = thrid % NUM_THREADS_PER_NODE;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg1 = arg1;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg2 = arg2;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp = &resp;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].op = op;

	//printf("\n-----------------> Combine %d\n", 0);

	do {
		while ((rds->local[rds->leader[thrid].val].replica->combinerLock.val != 0) && (((rds->local[rds->leader[thrid].val].replica->slot[myIndex].op != 0)))) {
			_mm_pause();
		}
		if (((rds->local[rds->leader[thrid].val].replica->slot[myIndex].op == 0))) {
			// Not combiner
			while (resp == MAX_UINT32) {
				_mm_pause();
			}
			return resp;
		}

		

		if ((CompareSwap32(&(rds->local[rds->leader[thrid].val].replica->combinerLock.val), 0, 1)) == 0) {
			if (rds->local[rds->leader[thrid].val].replica->slot[myIndex].op == 0) {
				redisAssert(resp != MAX_UINT32);
				rds->local[rds->leader[thrid].val].replica->combinerLock.val = 0;
				return resp;
			}
			

			// Combiner
			int howMany = 0;
			u32 index;
			for (index = 0; (index < NUM_THREADS_PER_NODE); ++index) {
				if (rds->local[rds->leader[thrid].val].replica->slot[index].op != EMPTY) {
					if (rds->local[rds->leader[thrid].val].replica->slot[index].op != CONTAINS) {
						++howMany;
					}
					rds->local[rds->leader[thrid].val].replica->slot[index].op |= (1 << CYCLE_BITS);
				}
			}
			

			NodeRWLock_Dist_WLock(&(rds->local[rds->leader[thrid].val].replica->lock));


			

			if (howMany > 0) {
				startInd = AppendAndUpdate(rds, thrid, howMany);
				counter = startInd;
				finalInd = ADDN(startInd, howMany);

				

				for (index = 0; (index < NUM_THREADS_PER_NODE); ++index) {
					if (((rds->local[rds->leader[thrid].val].replica->slot[index].op >> CYCLE_BITS) ^ 1) == 0) {
						nextOp = rds->local[rds->leader[thrid].val].replica->slot[index].op  & CYCLE_MASK;
						if (nextOp != CONTAINS) {
							rds->sharedLog.log[counter].arg1 = rds->local[rds->leader[thrid].val].replica->slot[index].arg1;
							rds->sharedLog.log[counter].arg2 = rds->local[rds->leader[thrid].val].replica->slot[index].arg2;

							if (counter < rds->local[rds->leader[thrid].val].replica->localTail) {
								rds->sharedLog.log[counter].op = nextOp | ((1 - rds->local[rds->leader[thrid].val].replica->localBit) << CYCLE_BITS);
							}
							else {
								rds->sharedLog.log[counter].op = nextOp | (rds->local[rds->leader[thrid].val].replica->localBit << CYCLE_BITS);
							}

							counter = INC(counter);
						}

					}
				}

			}
			else {
				finalInd = startInd = rds->sharedLog.logTail.val;
			}

			

			UpdateFromLog(rds, thrid, startInd);
			rds->local[rds->leader[thrid].val].replica->localTail = finalInd;
			if (rds->local[rds->leader[thrid].val].replica->localTail < startInd) rds->local[rds->leader[thrid].val].replica->localBit = 1 - rds->local[rds->leader[thrid].val].replica->localBit;
			

			for (index = 0; (index < NUM_THREADS_PER_NODE); ++index) {
				
				if (((rds->local[rds->leader[thrid].val].replica->slot[index].op >> CYCLE_BITS) ^ 1) == 0) {
					
					nextOp = rds->local[rds->leader[thrid].val].replica->slot[index].op  & CYCLE_MASK;
					rds->local[rds->leader[thrid].val].replica->slot[index].op = EMPTY;					
					*(rds->local[rds->leader[thrid].val].replica->slot[index].resp) = Execute_local(rds, thrid, nextOp, rds->local[rds->leader[thrid].val].replica->slot[index].arg1, rds->local[rds->leader[thrid].val].replica->slot[index].arg2);					
				}
			}


			

			if (howMany) UpdateMin(rds, thrid, startInd, howMany);
			

			NodeRWLock_Dist_WUnlock(&(rds->local[rds->leader[thrid].val].replica->lock));
			rds->local[rds->leader[thrid].val].replica->combinerLock.val = 0;
			

			return *(rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp);
		}
	} while (1);
}


inline bool BetweenLocalAndTail(RDS *rds, u32 readTail, u32 localTail, u32 tail) {	
	if (localTail <= tail) {
		return ((localTail < readTail) && (readTail <= tail));
	}
	else {
		return ((localTail < readTail) || (readTail <= tail));
	}
}


u32 CombineReads(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 readTail, resp;
	u32 id = thrid % NUM_THREADS_PER_NODE;

	
	if (NodeRWLock_Dist_TryRLock(&(rds->local[rds->leader[thrid].val].replica->lock), id)) { 
		readTail = rds->sharedLog.logTail.val;		
		if (!BetweenLocalAndTail(rds, readTail, rds->local[rds->leader[thrid].val].replica->localTail, rds->sharedLog.logTail.val)) {
		
		
			resp = RDS_contains_local(rds, thrid, arg1, arg2);
			NodeRWLock_Dist_RUnlock(&(rds->local[rds->leader[thrid].val].replica->lock), id);
			return resp;
		}
		NodeRWLock_Dist_RUnlock(&(rds->local[rds->leader[thrid].val].replica->lock), id);
	}


	//printf("\n-----------------> WRONG %d\n", 0);
	//return 0;

	return Combine(rds, thrid, op, arg1, arg2);
	
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


//TODO:SID NOT NEEDED?
/*
int RDS_size(RDS *rds) {
	SharedLog_Print(&rds->sharedLog);
	UpdateFromLog(rds, 0, rds->sharedLog.logTail.val);	
	return sl_set_size_local(rds->local[rds->leader[0].val].replica->localReg);
	//return 0;
}
*/

void RDS_Start(RDS *rds) {
	int i;
	//printf("\n-----------------> START %d\n", 0);
	SharedLog_Init(&(rds->sharedLog));
	for (i = 0; i < MAX_THREADS; ++i) rds->local[i].replica = NULL;
}

RDS* RDS_new() {
	RDS *rds = (RDS *)malloc(sizeof(RDS));
	//printf("\n-----------------> NEW %d\n", 1);
	RDS_Start(rds);
	thread_ids = dictCreate(&IntDictType, NULL);
	dictExpand(thread_ids, MAX_THREADS);
	return rds;
}

void IntraSocket(RDS *rds, int thrid) {
	u32 leaderT = (thrid / NUM_THREADS_PER_NODE) * NUM_THREADS_PER_NODE;
	rds->leader[thrid].val = leaderT;
	if (thrid == leaderT) {
		//printf("\n-----------------> Leader Allocating replica %d\n", thrid);
		rds->local[thrid].replica = (NodeReplica_OPTR*)malloc(sizeof(NodeReplica_OPTR));
		NodeReplica_OPTR_Init(rds->local[thrid].replica);
		//printf("create thrid %d leader %d\n", thrid, leaderT);
	}
	//printf("thrid %d leader %d\n", thrid, leaderT);
}

void RDS_StartThread(RDS *rds, int thrid) {
	//printf("-----------------> START %d\n", thrid);
	//printf("\n-----------------> START_THR %d\n", thrid);
	dictAdd(thread_ids, GetCurrentThreadId(), thrid);
	IntraSocket(rds, thrid);
}

u32 RDS_contains(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> CONTAINS %d\n", thrid);
	return CombineReads(rds, thrid, CONTAINS, arg1, arg2);
}

u32 RDS_insert(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> INSERT %d\n", thrid);
	return Combine(rds, thrid, INSERT, arg1, arg2);		
}

u32 RDS_incrby(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> INSERT %d\n", thrid);
	//TODO:RDS
	//SharedLog_Print(&rds->sharedLog);
	return Combine(rds, thrid, INCRBY, arg1, arg2);
}

u32 RDS_remove(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> REMOVE %d\n", thrid);
	return Combine(rds, thrid, REMOVE, arg1, arg2);
}



void RDS_FinishThread(RDS *rds, int thrid) {
	//local[leader[thrid].val].replica->localReg->printTree();
	//local[leader[thrid].val].replica->localReg->checkTree();
}

void RDS_Finish(RDS *rds) {
	SharedLog_Print(&rds->sharedLog);

	//for (int i = 0; i < 40; ++i) local[i].replica->localReg->Print(i);
}



