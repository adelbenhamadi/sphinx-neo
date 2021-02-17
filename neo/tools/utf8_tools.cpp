#include "neo/tools/utf8_tools.h"

namespace NEO {

	/*inline*/ int sphUTF8Decode(const BYTE*& pBuf)
	{
		BYTE v = *pBuf++;
		if (!v)
			return 0;

		// check for 7-bit case
		if (v < 128)
			return v;

		// get number of bytes
		int iBytes = 0;
		while (v & 0x80)
		{
			iBytes++;
			v <<= 1;
		}

		// check for valid number of bytes
		if (iBytes<2 || iBytes>SPH_MAX_UTF8_BYTES)
			return -1;

		int iCode = (v >> iBytes);
		iBytes--;
		do
		{
			if (!(*pBuf))
				return 0; // unexpected eof

			if (((*pBuf) & 0xC0) != 0x80)
				return -1; // invalid code

			iCode = (iCode << 6) + ((*pBuf) & 0x3F);
			iBytes--;
			pBuf++;
		} while (iBytes);

		// all good
		return iCode;
	}

	/*inline-*/ int sphUTF8Encode(BYTE* pBuf, int iCode)
	{
		if (iCode < 0x80)
		{
			pBuf[0] = (BYTE)(iCode & 0x7F);
			return 1;
		}

		if (iCode < 0x800)
		{
			pBuf[0] = (BYTE)(((iCode >> 6) & 0x1F) | 0xC0);
			pBuf[1] = (BYTE)((iCode & 0x3F) | 0x80);
			return 2;
		}

		if (iCode < 0x10000)
		{
			pBuf[0] = (BYTE)(((iCode >> 12) & 0x0F) | 0xE0);
			pBuf[1] = (BYTE)(((iCode >> 6) & 0x3F) | 0x80);
			pBuf[2] = (BYTE)((iCode & 0x3F) | 0x80);
			return 3;
		}

		pBuf[0] = (BYTE)(((iCode >> 18) & 0x0F) | 0xF0);
		pBuf[1] = (BYTE)(((iCode >> 12) & 0x3F) | 0x80);
		pBuf[2] = (BYTE)(((iCode >> 6) & 0x3F) | 0x80);
		pBuf[3] = (BYTE)((iCode & 0x3F) | 0x80);
		return 4;
	}


	// convert utf8 to unicode string
	int DecodeUtf8(const BYTE* sWord, int* pBuf)
	{
		if (!sWord)
			return 0;

		auto pCur = pBuf;
		while (*sWord)
		{
			*pCur = sphUTF8Decode(sWord);
			pCur++;
		}
		return (int)(pCur - pBuf);
	}

	int BuildUtf8Offsets(const char* sWord, int iLen, int* pOff, int DEBUGARG(iBufSize))
	{
		const BYTE* s = (const BYTE*)sWord;
		const BYTE* sEnd = s + iLen;
		auto* pStartOff = pOff;
		*pOff = 0;
		pOff++;
		while (s < sEnd)
		{
			sphUTF8Decode(s);
			*pOff =(int)( s - (const BYTE*)sWord);
			pOff++;
		}
		assert(pOff - pStartOff < iBufSize);
		return (int)(pOff - pStartOff - 1);
	}


	////////////////////
	// BINARY COLLATION
	////////////////////

	int sphCollateBinary(const BYTE* pStr1, const BYTE* pStr2, bool bPacked)
	{
		if (bPacked)
		{
			int iLen1 = sphUnpackStr(pStr1, &pStr1);
			int iLen2 = sphUnpackStr(pStr2, &pStr2);
			int iRes = memcmp((const char*)pStr1, (const char*)pStr2, Min(iLen1, iLen2));
			return iRes ? iRes : (iLen1 - iLen2);
		}
		else
		{
			return strcmp((const char*)pStr1, (const char*)pStr2);
		}
	}

	///////////////////////////////
	// LIBC_CI, LIBC_CS COLLATIONS
	///////////////////////////////

	/// initialize collation LUTs
	void sphCollationInit()
	{
		const int dWeightPlane[0x0b] = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x1e, 0x1f, 0x21, 0x24, 0xff };

		// generate missing weights
		for (int i = 0; i < 0x100; i++)
		{
			g_dCollWeights_UTF8CI[i + 0x800] = (unsigned short)(0x2100 + i - (i >= 0x70 && i <= 0x7f) * 16); // 2170..217f, -16
			g_dCollWeights_UTF8CI[i + 0x900] = (unsigned short)(0x2400 + i - (i >= 0xd0 && i <= 0xe9) * 26); // 24d0..24e9, -26
			g_dCollWeights_UTF8CI[i + 0xa00] = (unsigned short)(0xff00 + i - (i >= 0x41 && i <= 0x5a) * 32); // ff41..ff5a, -32
		}

		// generate planes table
		for (int i = 0; i < 0x100; i++)
			g_dCollPlanes_UTF8CI[i] = NULL;

		for (int i = 0; i < 0x0b; i++)
			g_dCollPlanes_UTF8CI[dWeightPlane[i]] = g_dCollWeights_UTF8CI + 0x100 * i;
	}

	/// utf8_general_ci
	int sphCollateUtf8GeneralCI(const BYTE* pArg1, const BYTE* pArg2, bool bPacked)
	{
		const BYTE* pStr1 = pArg1;
		const BYTE* pStr2 = pArg2;
		const BYTE* pMax1 = NULL;
		const BYTE* pMax2 = NULL;
		if (bPacked)
		{
			int iLen1 = sphUnpackStr(pStr1, (const BYTE**)&pStr1);
			int iLen2 = sphUnpackStr(pStr2, (const BYTE**)&pStr2);
			pMax1 = pStr1 + iLen1;
			pMax2 = pStr2 + iLen2;
		}

		while ((bPacked && pStr1 < pMax1 && pStr2 < pMax2) || (!bPacked && *pStr1 && *pStr2))
		{
			// FIXME! on broken data, decode might go beyond buffer bounds
			int iCode1 = sphUTF8Decode(pStr1);
			int iCode2 = sphUTF8Decode(pStr2);
			if (!iCode1 && !iCode2)
				return 0;
			if (!iCode1 || !iCode2)
				return !iCode1 ? -1 : 1;

			if (iCode1 == iCode2)
				continue;
			iCode1 = CollateUTF8CI(iCode1);
			iCode2 = CollateUTF8CI(iCode2);
			if (iCode1 != iCode2)
				return iCode1 - iCode2;
		}

		if (bPacked)
		{
			if (pStr1 >= pMax1 && pStr2 >= pMax2)
				return 0;
			return (pStr1 < pMax1) ? 1 : -1;
		}
		else
		{
			if (!*pStr1 && !*pStr2)
				return 0;
			return (*pStr1 ? 1 : -1);
		}
	}

	/// libc_ci, wrapper for strcasecmp
	int sphCollateLibcCI(const BYTE* pStr1, const BYTE* pStr2, bool bPacked)
	{
		if (bPacked)
		{
			int iLen1 = sphUnpackStr(pStr1, &pStr1);
			int iLen2 = sphUnpackStr(pStr2, &pStr2);
			int iRes = strncasecmp((const char*)pStr1, (const char*)pStr2, Min(iLen1, iLen2));
			return iRes ? iRes : (iLen1 - iLen2);
		}
		else
		{
			return strcasecmp((const char*)pStr1, (const char*)pStr2);
		}
	}

	/// libc_cs, wrapper for strcoll
	int sphCollateLibcCS(const BYTE* pStr1, const BYTE* pStr2, bool bPacked)
	{

		if (bPacked)
		{
			int iLen1 = sphUnpackStr(pStr1, &pStr1);
			int iLen2 = sphUnpackStr(pStr2, &pStr2);

			// strcoll wants asciiz strings, so we would have to copy them over
			// lets use stack buffer for smaller ones, and allocate from heap for bigger ones
			int iRes = 0;
			int iLen = Min(iLen1, iLen2);
			if (iLen < COLLATE_STACK_BUFFER)
			{
				// small strings on stack
				BYTE sBuf1[COLLATE_STACK_BUFFER];
				BYTE sBuf2[COLLATE_STACK_BUFFER];

				memcpy(sBuf1, pStr1, iLen);
				memcpy(sBuf2, pStr2, iLen);
				sBuf1[iLen] = sBuf2[iLen] = '\0';
				iRes = strcoll((const char*)sBuf1, (const char*)sBuf2);
			}
			else
			{
				// big strings on heap
				char* pBuf1 = new char[iLen];
				char* pBuf2 = new char[iLen];

				memcpy(pBuf1, pStr1, iLen);
				memcpy(pBuf2, pStr2, iLen);
				pBuf1[iLen] = pBuf2[iLen] = '\0';
				iRes = strcoll((const char*)pBuf1, (const char*)pBuf2);

				SafeDeleteArray(pBuf2);
				SafeDeleteArray(pBuf1);
			}

			return iRes ? iRes : (iLen1 - iLen2);
		}
		else
		{
			return strcoll((const char*)pStr1, (const char*)pStr2);
		}
	}

}
