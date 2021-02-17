#pragma once
#include "neo/int/types.h"

namespace NEO {

	template < typename DOCID >
	inline DOCID DOCINFO2ID_T(const DWORD* pDocinfo);

	template<> inline DWORD DOCINFO2ID_T(const DWORD* pDocinfo)
	{
		return pDocinfo[0];
	}

	template<> inline uint64_t DOCINFO2ID_T(const DWORD* pDocinfo)
	{
#if USE_LITTLE_ENDIAN
		return uint64_t(pDocinfo[0]) + (uint64_t(pDocinfo[1]) << 32);
#else
		return uint64_t(pDocinfo[1]) + (uint64_t(pDocinfo[0]) << 32);
#endif
	}

	inline void DOCINFOSETID(DWORD* pDocinfo, DWORD uValue)
	{
		*pDocinfo = uValue;
	}

	inline void DOCINFOSETID(DWORD* pDocinfo, uint64_t uValue)
	{
#if USE_LITTLE_ENDIAN
		pDocinfo[0] = (DWORD)uValue;
		pDocinfo[1] = (DWORD)(uValue >> 32);
#else
		pDocinfo[0] = (DWORD)(uValue >> 32);
		pDocinfo[1] = (DWORD)uValue;
#endif
	}

	inline SphDocID_t DOCINFO2ID(const DWORD* pDocinfo)
	{
		return DOCINFO2ID_T<SphDocID_t>(pDocinfo);
	}

#if PARANOID
	template < typename DOCID > inline DWORD* DOCINFO2ATTRS_T(DWORD* pDocinfo) { assert(pDocinfo); return pDocinfo + DWSIZEOF(DOCID); }
	template < typename DOCID > inline const DWORD* DOCINFO2ATTRS_T(const DWORD* pDocinfo) { assert(pDocinfo); return pDocinfo + DWSIZEOF(DOCID); }
	template < typename DOCID > inline DWORD* STATIC2DOCINFO_T(DWORD* pAttrs) { assert(pAttrs); return pAttrs - DWSIZEOF(DOCID); }
	template < typename DOCID > inline const DWORD* STATIC2DOCINFO_T(const DWORD* pAttrs) { assert(pAttrs); return pAttrs - DWSIZEOF(DOCID); }
#else
	template < typename DOCID > inline DWORD* DOCINFO2ATTRS_T(DWORD* pDocinfo) { return pDocinfo + DWSIZEOF(DOCID); }
	template < typename DOCID > inline const DWORD* DOCINFO2ATTRS_T(const DWORD* pDocinfo) { return pDocinfo + DWSIZEOF(DOCID); }
	template < typename DOCID > inline DWORD* STATIC2DOCINFO_T(DWORD* pAttrs) { return pAttrs - DWSIZEOF(DOCID); }
	template < typename DOCID > inline const DWORD* STATIC2DOCINFO_T(const DWORD* pAttrs) { return pAttrs - DWSIZEOF(DOCID); }
#endif

	inline 			DWORD* DOCINFO2ATTRS(DWORD* pDocinfo) { return DOCINFO2ATTRS_T<SphDocID_t>(pDocinfo); }
	inline const	DWORD* DOCINFO2ATTRS(const DWORD* pDocinfo) { return DOCINFO2ATTRS_T<SphDocID_t>(pDocinfo); }
	inline 			DWORD* STATIC2DOCINFO(DWORD* pAttrs) { return STATIC2DOCINFO_T<SphDocID_t>(pAttrs); }
	inline const	DWORD* STATIC2DOCINFO(const DWORD* pAttrs) { return STATIC2DOCINFO_T<SphDocID_t>(pAttrs); }


	//////////////////
	extern const int DOCLIST_HINT_THRESH;

	// let uDocs be DWORD here to prevent int overflow in case of hitless word (highest bit is 1)
	static int DoclistHintUnpack(DWORD uDocs, BYTE uHint)
	{
		if (uDocs < (DWORD)DOCLIST_HINT_THRESH)
			return (int)Min(8 * (int64_t)uDocs, INT_MAX);
		else
			return (int)Min(4 * (int64_t)uDocs + (int64_t(uDocs) * uHint / 64), INT_MAX);
	}

	BYTE sphDoclistHintPack(SphOffset_t iDocs, SphOffset_t iLen);


}