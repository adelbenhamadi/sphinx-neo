#pragma once
#include "neo/int/types.h"


inline int encodeVLB(BYTE* buf, DWORD v)
{
	register BYTE b;
	register int n = 0;

	do
	{
		b = (BYTE)(v & 0x7f);
		v >>= 7;
		if (v)
			b |= 0x80;
		*buf++ = b;
		n++;
	} while (v);
	return n;
}


inline int encodeKeyword(BYTE* pBuf, const char* pKeyword)
{
	int iLen = strlen(pKeyword); // OPTIMIZE! remove this and memcpy and check if thats faster
	assert(iLen > 0 && iLen < 128); // so that ReadVLB()

	*pBuf = (BYTE)iLen;
	memcpy(pBuf + 1, pKeyword, iLen);
	return 1 + iLen;
}


inline int sphEncodeVLB8(BYTE* buf, uint64_t v)
{
	register BYTE b;
	register int n = 0;

	do
	{
		b = (BYTE)(v & 0x7f);
		v >>= 7;
		if (v)
			b |= 0x80;
		*buf++ = b;
		n++;
	} while (v);
	return n;
}


inline const BYTE* spnDecodeVLB8(const BYTE* pIn, uint64_t& uValue)
{
	BYTE bIn;
	int iOff = 0;

	do
	{
		bIn = *pIn++;
		uValue += (uint64_t(bIn & 0x7f)) << iOff;
		iOff += 7;
	} while (bIn & 0x80);

	return pIn;
}
