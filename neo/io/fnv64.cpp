#include "neo/io/fnv64.h"


namespace NEO {

	uint64_t sphFNV64(const void* s)
	{
		return sphFNV64cont(s, SPH_FNV64_SEED);
	}


	uint64_t sphFNV64(const void* s, size_t iLen, uint64_t uPrev)
	{
		const BYTE* p = (const BYTE*)s;
		uint64_t hval = uPrev;
		for (; iLen > 0; iLen--)
		{
			// xor the bottom with the current octet
			hval ^= (uint64_t)*p++;

			// multiply by the 64 bit FNV magic prime mod 2^64
			hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40); // gcc optimization
		}
		return hval;
	}


	uint64_t sphFNV64cont(const void* s, uint64_t uPrev)
	{
		const BYTE* p = (const BYTE*)s;
		if (!p)
			return uPrev;

		uint64_t hval = uPrev;
		while (*p)
		{
			// xor the bottom with the current octet
			hval ^= (uint64_t)*p++;

			// multiply by the 64 bit FNV magic prime mod 2^64
			hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40); // gcc optimization
		}
		return hval;
	}

}