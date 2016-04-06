#include "utility.h"

__declspec(thread) u16 lfsr = 0xACE1u;
__declspec(thread) u16 bit;

u16 randLFSR()
{
	bit = ((lfsr >> 0) ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5)) & 1;
	return lfsr = (lfsr >> 1) | (bit << 15);
}

void pinThread(u32 proc)
{
	// This only works if there are fewer than 64 procs
	//SetThreadAffinityMask(tHandle, (u64)1 << proc);
	GROUP_AFFINITY ga;
	ZeroMemory(&ga, sizeof(GROUP_AFFINITY));
	u16 groupcnt = GetActiveProcessorCount(0);
	u16 group = proc / groupcnt;

	ga.Group = group;
	// Affinity is specified relative to the processor group	
	ga.Mask = (u64)1 << (proc - (group * groupcnt));

	int ret = SetThreadGroupAffinity(GetCurrentThread(), &ga, NULL);
	if (!ret) printf("Error setting group affinity: %d (%d)\n", GetLastError(), proc);

	//printf("%d %d %d %d %d %d %d\n", ret, proc, group, proc - (group * groupcnt), GetActiveProcessorCount(group), GetActiveProcessorGroupCount(), GetMaximumProcessorGroupCount());
}
