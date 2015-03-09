#include "utility.h"

//TODO:SID ADDING TO HEADER SO CAN BE PROPERLY INLINED (OTHERWISE UNRESOLVED EXTERNAL)
/*
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
*/