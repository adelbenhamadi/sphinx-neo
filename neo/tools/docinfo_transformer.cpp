#include "neo/tools/docinfo_transformer.h"

namespace NEO {

	/*static*/ const int DOCLIST_HINT_THRESH = 256;

	BYTE sphDoclistHintPack(SphOffset_t iDocs, SphOffset_t iLen)
	{
		// we won't really store a hint for small lists
		if (iDocs < DOCLIST_HINT_THRESH)
			return 0;

		// for bigger lists len/docs varies 4x-6x on test indexes
		// so lets assume that 4x-8x should be enough for everybody
		SphOffset_t iDelta = Min(Max(iLen - 4 * iDocs, 0), 4 * iDocs - 1); // len delta over 4x, clamped to [0x..4x) range
		BYTE uHint = (BYTE)(64 * iDelta / iDocs); // hint now must be in [0..256) range
		while (uHint < 255 && (iDocs * uHint / 64) < iDelta) // roundoff (suddenly, my guru math skillz failed me)
			uHint++;

		return uHint;
	}

}