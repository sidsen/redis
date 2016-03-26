#include "utility.h"

__declspec(thread) u16 lfsr = 0xACE1u;
__declspec(thread) u16 bit;

u16 randLFSR()
{
	bit = ((lfsr >> 0) ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5)) & 1;
	return lfsr = (lfsr >> 1) | (bit << 15);
}