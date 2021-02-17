#include "neo/platform/random.h"
#include "neo/int/types.h"

namespace NEO {

	/// MWC (Multiply-With-Carry) RNG, invented by George Marsaglia
	/*static*/ DWORD g_dRngState[5] = { 0x95d3474bUL, 0x035cf1f7UL, 0xfd43995fUL, 0x5dfc55fbUL, 0x334a9229UL };


	/// seed
	void sphSrand(DWORD uSeed)
	{
		for (int i = 0; i < 5; i++)
		{
			uSeed = uSeed * 29943829 - 1;
			g_dRngState[i] = uSeed;
		}
		for (int i = 0; i < 19; i++)
			sphRand();
	}


	/// auto-seed RNG based on time and PID
	void sphAutoSrand()
	{
		// get timestamp
#if !USE_WINDOWS
		struct timeval tv;
		gettimeofday(&tv, NULL);
#else
#define getpid() GetCurrentProcessId()

		struct
		{
			time_t	tv_sec;
			DWORD	tv_usec;
		} tv;

		FILETIME ft;
		GetSystemTimeAsFileTime(&ft);

		uint64_t ts = (uint64_t(ft.dwHighDateTime) << 32) + uint64_t(ft.dwLowDateTime) - 116444736000000000ULL; // Jan 1, 1970 magic
		ts /= 10; // to microseconds
		tv.tv_sec = (DWORD)(ts / 1000000);
		tv.tv_usec = (DWORD)(ts % 1000000);
#endif

		// twist and shout
		sphSrand(sphRand() ^ DWORD(tv.tv_sec) ^ (DWORD(tv.tv_usec) + DWORD(getpid())));
	}


	/// generate another dword
	DWORD sphRand()
	{
		uint64_t uSum;
		uSum =
			(uint64_t)g_dRngState[0] * (uint64_t)5115 +
			(uint64_t)g_dRngState[1] * (uint64_t)1776 +
			(uint64_t)g_dRngState[2] * (uint64_t)1492 +
			(uint64_t)g_dRngState[3] * (uint64_t)2111111111UL +
			(uint64_t)g_dRngState[4];
		g_dRngState[3] = g_dRngState[2];
		g_dRngState[2] = g_dRngState[1];
		g_dRngState[1] = g_dRngState[0];
		g_dRngState[4] = (DWORD)(uSum >> 32);
		g_dRngState[0] = (DWORD)uSum;
		return g_dRngState[0];
	}

}