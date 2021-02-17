#pragma once
#include "neo/platform/compat.h"
#include "neo/core/globals.h"
#include "neo/int/types.h"


#include <cassert>
#include <climits>
#include <ctype.h>
#include <cstdint>

namespace NEO {

	//fwd dec
	//typedef 		SphWordID_t;
	//typedef 		SphDocID_t;


	/// float vs dword conversion
	inline DWORD sphF2DW(float f) { union { float f; DWORD d; } u; u.f = f; return u.d; }

	/// dword vs float conversion
	inline float sphDW2F(DWORD d) { union { float f; DWORD d; } u; u.d = d; return u.f; }

	/// double to bigint conversion
	inline uint64_t sphD2QW(double f) { union { double f; uint64_t d; } u; u.f = f; return u.d; }

	/// bigint to double conversion
	inline double sphQW2D(uint64_t d) { union { double f; uint64_t d; } u; u.d = d; return u.f; }



	/// double argument squared
	inline double sqr(double v) { return v * v; }

	/// float argument squared
	inline float fsqr(float v) { return v * v; }



#if !USE_WINDOWS
	char* strlwr(char* s)
	{
		while (*s)
		{
			*s = tolower(*s);
			s++;
		}
		return s;
	}
#endif


	static char* sphStrMacro(const char* sTemplate, const char* sMacro, SphDocID_t uValue)
	{
		// expand macro
		char sExp[32];
		snprintf(sExp, sizeof(sExp), DOCID_FMT, uValue);

		// calc lengths
		auto iExp = strlen(sExp);
		auto iMacro = strlen(sMacro);
		auto iDelta = iExp - iMacro;

		// calc result length
		auto iRes = strlen(sTemplate);
		const char* sCur = sTemplate;
		while ((sCur = strstr(sCur, sMacro)) != NULL)
		{
			iRes += iDelta;
			sCur++;
		}

		// build result
		char* sRes = new char[iRes + 1];
		char* sOut = sRes;
		const char* sLast = sTemplate;
		sCur = sTemplate;

		while ((sCur = strstr(sCur, sMacro)) != NULL)
		{
			strncpy(sOut, sLast, sCur - sLast); sOut += sCur - sLast;
			strcpy(sOut, sExp); sOut += iExp; // NOLINT
			sCur += iMacro;
			sLast = sCur;
		}

		if (*sLast)
			strcpy(sOut, sLast); // NOLINT

		assert((int)strlen(sRes) == iRes);
		return sRes;
	}


	static float sphToFloat(const char* s)
	{
		if (!s) return 0.0f;
		return (float)strtod(s, NULL);
	}


	static DWORD sphToDword(const char* s)
	{
		if (!s) return 0;
		return strtoul(s, NULL, 10);
	}


	static uint64_t sphToUint64(const char* s)
	{
		if (!s) return 0;
		return strtoull(s, NULL, 10);
	}


	static int64_t sphToInt64(const char* s)
	{
		if (!s) return 0;
		return strtoll(s, NULL, 10);
	}


#if USE_64BIT
#define sphToDocid sphToUint64
#else
#define sphToDocid sphToDword
#endif

}