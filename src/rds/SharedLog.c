#include "SharedLog.h"

//TODO:SID MOVING THESE TO HEADER FILE SO THEY CAN BE INLINED PROPERLY (LINKER ERROR OTHERWISE)
/*
inline void SharedLog_Init(SharedLog *sl) {
	int i;
	//printf("\n-----------------> SHAREDLOG_INIT %d\n", 0);
	for (i = 0; i < _logSize; ++i) sl->log[i].op = 0;
	sl->logTail.val = 0;
	sl->logMin.val = _logSize - 1;
}

inline void SharedLog_Print(SharedLog *sl) {
	printf("current log size %u\n", sl->logTail.val);
	//for (int i = 0; i < logTail; ++i) printf("%u %u\n", log[i].op, log[i].arg);
}

// use this to get one or more slots in the log
inline u32 SharedLog_GetNextTail(SharedLog *sl, u32 howMany) {
	u32 tail;
	u32 nextTail;
	u32 min, p;
	do {
		min = sl->logMin.val;
		p = (min + _logSize - MAX_COMBINE - 1) & (_logSize - 1);
		tail = sl->logTail.val;
		nextTail = ADDN(tail, howMany);
		if ((p < min) && (tail > p) && (tail < min)) return _logSize;
		if ((p > min) && ((tail > p) || (tail < min))) return _logSize;
	} while (CompareSwap32(&(sl->logTail.val), tail, nextTail) != tail);
	return tail;
}

// use this to get one slot in the log
inline u32 SharedLog_GetNextTail1(SharedLog *sl) {
	u32 tail;
	u32 nextTail;
	do {
		tail = sl->logTail.val;
		nextTail = INC(tail);
		if ((nextTail == sl->logMin.val)) {
			return _logSize;
		}
	} while (CompareSwap32(&(sl->logTail.val), tail, nextTail) != tail);
	return tail;
}
*/