#pragma once
#include "neo/int/types.h"


namespace NEO {

	/// Sphinx FNV64 implementation
	const uint64_t	SPH_FNV64_SEED = 0xcbf29ce484222325ULL;
	uint64_t		sphFNV64(const void* pString);
	uint64_t		sphFNV64(const void* s, size_t iLen, uint64_t uPrev = SPH_FNV64_SEED);
	uint64_t		sphFNV64cont(const void* pString, uint64_t uPrev);

}