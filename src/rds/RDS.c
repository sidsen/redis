#include "RDS.h"
//TODO:RDS
#include "..\redis.h"


//TODO:RDS TEMPORARILY CREATE GLOBAL 
RDS* rds;
dict* thread_ids;

/* Hash type hash table (note that small hashes are represented with ziplists) */
#if not defined(IntDictType)
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
	/*
	for (int i = 1; i < 500; i++) {
		_mm_pause();
	}
	return 1;
	*/
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
	case INSERT:
		RDS_insert_local(rds, thrid, arg1, arg2);
		break;
	case REMOVE:
		RDS_remove_local(rds, thrid, arg1, arg2);
		break;
	case INCRBY:
		RDS_incrby_local(rds, thrid, arg1, arg2);
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
	p = (logMin + _logSize - NUM_THREADS_PER_NODE - 1) & (_logSize - 1);

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
		p = (logMin + _logSize - NUM_THREADS_PER_NODE - 1) & (_logSize - 1);
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


//TODO:PERF FOR TESTING RDS PERF
// TODO(irina): took this out while updating the Combine method
#if 0

u32 Combine_incrby(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;
	volatile u32 resp = MAX_UINT32;
	u32 myIndex = thrid % NUM_THREADS_PER_NODE;

	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg1 = arg1;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg2 = arg2;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp = &resp;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].op = op;

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

#endif

#if 0
u32 Combine_incrby(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;
	volatile u32 resp = MAX_UINT32;
	u32 myIndex = thrid % NUM_THREADS_PER_NODE;

	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg1 = arg1;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg2 = arg2;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp = &resp;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].op = op;

	//printf("\n-----------------> Combine %d\n", 0);

	//printf("Thread %d is running on processor %d\n", thrid, GetCurrentProcessorNumber());
	//fflush(stdout);
	/*
	do {
		while (rds->local[rds->leader[thrid].val].replica->combinerLock.val != 0) {
			_mm_pause();
		}
		if ((CompareSwap32(&(rds->local[rds->leader[thrid].val].replica->combinerLock.val), 0, 1)) == 0) {
			//for (int i = 0; i < 5000; i++)
			//	_mm_pause();
			resp = Execute_local(rds, thrid, op, rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg1, rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg2);
			rds->local[rds->leader[thrid].val].replica->combinerLock.val = 0;
			return resp;
		}
	} while (1);
	*/

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

			// /*
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
			
			//*/

			for (index = 0; (index < NUM_THREADS_PER_NODE); ++index) {
				
				if (((rds->local[rds->leader[thrid].val].replica->slot[index].op >> CYCLE_BITS) ^ 1) == 0) {
					
					nextOp = rds->local[rds->leader[thrid].val].replica->slot[index].op  & CYCLE_MASK;
					rds->local[rds->leader[thrid].val].replica->slot[index].op = EMPTY;					
					*(rds->local[rds->leader[thrid].val].replica->slot[index].resp) = Execute_local(rds, thrid, nextOp, rds->local[rds->leader[thrid].val].replica->slot[index].arg1, rds->local[rds->leader[thrid].val].replica->slot[index].arg2);					
				}
			}



			//
			if (howMany) UpdateMin(rds, thrid, startInd, howMany);
			

			//
			NodeRWLock_Dist_WUnlock(&(rds->local[rds->leader[thrid].val].replica->lock));
			rds->local[rds->leader[thrid].val].replica->combinerLock.val = 0;

			return *(rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp);
		}
	} while (1);
}
#endif

inline bool BetweenLocalAndTail(RDS *rds, u32 readTail, u32 localTail, u32 tail) {	
	if (localTail <= tail) {
		return ((localTail < readTail) && (readTail <= tail));
	}
	else {
		return ((localTail < readTail) || (readTail <= tail));
	}
}

static const u32 COMBINER_HOLD_TIME_MS = 5;
//IRINA: Is this sufficient, or does the timer need to be volatile as well?
volatile bool combinerHold = false;
volatile u64 combinerStart = 0;
u32 combinerId = 0;

u32 Combine(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 nextOp;
	u32 counter, startInd, finalInd;	
	u32 myIndex = thrid % NUM_THREADS_PER_NODE;

	rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val = MAX_UINT32;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg1 = arg1;
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].arg2 = arg2;	
	rds->local[rds->leader[thrid].val].replica->slot[myIndex].op = op;

	do {

		//<SID-OPT>
		// This allows any thread to release the combiner hold. We are not protecting this code
		// with a lock, but it should not result in corrupt state and in the worst case 
		// multiple threads will attempt to grab the combiner lock below
		if (combinerHold && ((ustime() - combinerStart) / 1000.0 > COMBINER_HOLD_TIME_MS)) {
			combinerHold = false;
			// This is superfluous since we've already read the elapsed time, but include
			// it to avoid confusion
			combinerStart = 0;
		}
		//<SID-OPT>

		if ((rds->local[rds->leader[thrid].val].replica->combinerLock.val == 0) 
			&& (rds->local[rds->leader[thrid].val].replica->combinerLock.val == 0)
			&& (rds->local[rds->leader[thrid].val].replica->combinerLock.val == 0)
			&& (((combinerHold && combinerId == myIndex) || !combinerHold) &&
			(CompareSwap32(&(rds->local[rds->leader[thrid].val].replica->combinerLock.val), 0, 1) == 0))) {

			// I am the Combiner

			//<SID-OPT>
			// Start the combiner hold timer if it hasn't been started already
			if (!combinerHold) {
				//SID-OPT: DISABLE THE OPTIMIZATION FOR NOW
				combinerHold = true;
				combinerId = myIndex;
				combinerStart = ustime();
			}
			//<SID-OPT>

			int howMany = 0;
			u32 index;
			for (index = 0; (index < rds->local[rds->leader[thrid].val].replica->threadCnt); ++index) {
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



				for (index = 0; (index < rds->local[rds->leader[thrid].val].replica->threadCnt); ++index) {
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


			for (index = 0; (index < rds->local[rds->leader[thrid].val].replica->threadCnt); ++index) {

				if (((rds->local[rds->leader[thrid].val].replica->slot[index].op >> CYCLE_BITS) ^ 1) == 0) {

					nextOp = rds->local[rds->leader[thrid].val].replica->slot[index].op  & CYCLE_MASK;
					rds->local[rds->leader[thrid].val].replica->slot[index].op = EMPTY;
					rds->local[rds->leader[thrid].val].replica->slot[index].resp.val = Execute_local(rds, thrid, nextOp, rds->local[rds->leader[thrid].val].replica->slot[index].arg1, rds->local[rds->leader[thrid].val].replica->slot[index].arg2);
				}
			}


			if (howMany) UpdateMin(rds, thrid, startInd, howMany);

			NodeRWLock_Dist_WUnlock(&(rds->local[rds->leader[thrid].val].replica->lock));
			rds->local[rds->leader[thrid].val].replica->combinerLock.val = 0;

			redisAssert(rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val != MAX_UINT32);
			return rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val;
		}
		else {
			// not combiner, wait for response or wait to become combiner

			while ((rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val == MAX_UINT32) 
				&&  (rds->local[rds->leader[thrid].val].replica->combinerLock.val != 0)) {
				_mm_pause();
			}

			if (rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val != MAX_UINT32) {
				return rds->local[rds->leader[thrid].val].replica->slot[myIndex].resp.val;
			}
		}
			
	} while (1);
}


u32 CombineReads(RDS *rds, int thrid, u32 op, u32 arg1, u32 arg2) {
	u32 readTail, resp;
	u32 id = thrid % NUM_THREADS_PER_NODE;

	
	//if (NodeRWLock_Dist_TryRLock(&(rds->local[rds->leader[thrid].val].replica->lock), id)) { 
	//TODO:PERF THIS SEEMS TO HELP A LOT FOR 99% READS
	NodeRWLock_Dist_RLock(&(rds->local[rds->leader[thrid].val].replica->lock), id);
		readTail = rds->sharedLog.logTail.val;		
		if (!BetweenLocalAndTail(rds, readTail, rds->local[rds->leader[thrid].val].replica->localTail, rds->sharedLog.logTail.val)) {
		
		
			resp = RDS_contains_local(rds, thrid, arg1, arg2);
			NodeRWLock_Dist_RUnlock(&(rds->local[rds->leader[thrid].val].replica->lock), id);
			return resp;
		}
		NodeRWLock_Dist_RUnlock(&(rds->local[rds->leader[thrid].val].replica->lock), id);
	//}


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
	nr->threadCnt = 0;
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
	rds->threadCnt = 0;
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
	// Keep track of the total number of threads registered
	AtomicInc32(&rds->threadCnt);
	//printf("thrid %d leader %d\n", thrid, leaderT);
}

void RDS_StartThread(RDS *rds, int thrid) {
	//printf("-----------------> START %d\n", thrid);
	//printf("\n-----------------> START_THR %d\n", thrid);
	dictAdd(thread_ids, GetCurrentThreadId(), thrid);
	IntraSocket(rds, thrid);
}

// This should be called *after* all threads have registered with RDS. It assumes thread
// ids are assigned contiguously starting from 0. This method is not thread safe.
void RDS_SetReplicaThreadCounts(RDS *rds) {
	for (int i = 0; i < rds->threadCnt; i++) {
		rds->local[rds->leader[i].val].replica->threadCnt++;
	}
}

u32 RDS_contains(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> CONTAINS %d\n", thrid);
	//SharedLog_Print(&rds->sharedLog);
	return CombineReads(rds, thrid, CONTAINS, arg1, arg2);
}

u32 RDS_insert(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> INSERT %d\n", thrid);
	return Combine(rds, thrid, INSERT, arg1, arg2);		
}

u32 RDS_incrby(RDS *rds, int thrid, u32 arg1, u32 arg2) {
	//printf("\n-----------------> INSERT %d\n", thrid);
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



