#pragma once
#include "neo/index/enums.h"
#include "neo/int/string.h"
#include "neo/core/globals.h"

#include <cassert>
 
namespace NEO {

	const char* sphTypeName(ESphAttr eType);

	const char* sphTypeDirective(ESphAttr eType);

	void SqlUnescape(CSphString& sRes, const char* sEscaped, int iLen);

	void StripPath(CSphString& sPath);

	size_t sphAddMva64(CSphVector<DWORD>& dStorage, int64_t iVal);



#if UNALIGNED_RAM_ACCESS

	/// pass-through wrapper
	template < typename T > inline T sphUnalignedRead(const T& tRef)
	{
		return tRef;
	}

	/// pass-through wrapper
	template < typename T > void sphUnalignedWrite(void* pPtr, const T& tVal)
	{
		*(T*)pPtr = tVal;
	}

#else

	/// unaligned read wrapper for some architectures (eg. SPARC)
	template < typename T >
	inline T sphUnalignedRead(const T& tRef)
	{
		T uTmp;
		BYTE* pSrc = (BYTE*)&tRef;
		BYTE* pDst = (BYTE*)&uTmp;
		for (int i = 0; i < (int)sizeof(T); i++)
			*pDst++ = *pSrc++;
		return uTmp;
	}

	/// unaligned write wrapper for some architectures (eg. SPARC)
	template < typename T >
	void sphUnalignedWrite(void* pPtr, const T& tVal)
	{
		BYTE* pDst = (BYTE*)pPtr;
		BYTE* pSrc = (BYTE*)&tVal;
		for (int i = 0; i < (int)sizeof(T); i++)
			*pDst++ = *pSrc++;
	}

#endif // unalgined


#if UNALIGNED_RAM_ACCESS && USE_LITTLE_ENDIAN
	/// get a dword from memory, intel version
	inline DWORD sphGetDword(const BYTE* p)
	{
		return *(const DWORD*)p;
	}
#else
	/// get a dword from memory, non-intel version
	inline DWORD sphGetDword(const BYTE* p)
	{
		return p[0] + (p[1] << 8) + (p[2] << 16) + (p[3] << 24);
	}
#endif

	//remove declared in utf8_tools.h with inline
	//int sphUTF8Len ( const char * pStr );

	/// check for valid attribute name char
	inline int sphIsAttr(int c)
	{
		// different from sphIsAlpha() in that we don't allow minus
		return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
	}

	///////////////////////////////

	inline void FlipEndianess(DWORD* pData)
	{
		BYTE* pB = (BYTE*)pData;
		BYTE a = pB[0];
		pB[0] = pB[3];
		pB[3] = a;
		a = pB[1];
		pB[1] = pB[2];
		pB[2] = a;
	};

	/*static*/inline const char* CheckFmtMagic(DWORD uHeader)
	{
		if (uHeader != INDEX_MAGIC_HEADER)
		{
			FlipEndianess(&uHeader);
			if (uHeader == INDEX_MAGIC_HEADER)
#if USE_LITTLE_ENDIAN
				return "This instance is working on little-endian platform, but %s seems built on big-endian host.";
#else
				return "This instance is working on big-endian platform, but %s seems built on little-endian host.";
#endif
			else
				return "%s is invalid header file (too old index version?)";
		}
		return NULL;
	}

	////////////////////////////

	/*static*/ inline int ZippedIntSize(DWORD v)
	{
		if (v < (1UL << 7))
			return 1;
		if (v < (1UL << 14))
			return 2;
		if (v < (1UL << 21))
			return 3;
		if (v < (1UL << 28))
			return 4;
		return 5;
	}


	inline int FindBit(DWORD uValue)
	{
		DWORD uMask = 0xffff;
		int iIdx = 0;
		int iBits = 16;

		// we negate bits to compare with 0
		// this makes MSVC emit 'test' instead of 'cmp'
		uValue ^= 0xffffffff;
		for (int t = 0; t < 5; t++)
		{
			if ((uValue & uMask) == 0)
			{
				iIdx += iBits;
				uValue >>= iBits;
			}
			iBits >>= 1;
			uMask >>= iBits;
		}
		return iIdx;
	}

	/////////////////////////////
	// logf() is not there sometimes (eg. Solaris 9)
#if !USE_WINDOWS && !HAVE_LOGF
	static inline float logf(float v)
	{
		return (float)log(v);
	}
#endif



#if USE_WINDOWS
	void localtime_r(const time_t* clock, struct tm* res);
	void gmtime_r(const time_t* clock, struct tm* res);
#endif

}