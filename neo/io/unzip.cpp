#include "neo/io/unzip.h"

namespace NEO {

	DWORD sphUnzipInt(const BYTE*& pBuf) { SPH_VARINT_DECODE(DWORD, *pBuf++); }
	SphOffset_t sphUnzipOffset(const BYTE*& pBuf) { SPH_VARINT_DECODE(SphOffset_t, *pBuf++); }

}