#pragma once
#include "neo/int/types.h"

namespace NEO {
	extern DWORD	g_dSphinxCRC32[256];

	DWORD sphCRC32(const void* s);
	DWORD sphCRC32(const void* s, int iLen);

	DWORD sphCRC32(const void* s, int iLen, DWORD uPrevCRC);

	/// calculate file crc32
	bool			sphCalcFileCRC32(const char* szFilename, DWORD& uCRC32);
}