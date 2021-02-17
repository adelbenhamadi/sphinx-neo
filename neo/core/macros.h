#pragma once
#include "neo/index/enums.h"

#define SafeDelete(_x)		{ if (_x) { delete (_x); (_x) = nullptr; } }
#define SafeDeleteArray(_x)	{ if (_x) { delete [] (_x); (_x) = nullptr; } }
#define SafeRelease(_x)		{ if (_x) { (_x)->Release(); (_x) = nullptr; } }



#if USE_WINDOWS
#define DISABLE_CONST_COND_CHECK \
	__pragma ( warning ( push ) ) \
	__pragma ( warning ( disable:4127 ) )
#define ENABLE_CONST_COND_CHECK \
	__pragma ( warning ( pop ) )
#else
#define DISABLE_CONST_COND_CHECK
#define ENABLE_CONST_COND_CHECK
#endif

#define if_const(_arg) \
	DISABLE_CONST_COND_CHECK \
	if ( _arg ) \
	ENABLE_CONST_COND_CHECK


///////////////


namespace NEO {

	class CSphIndex_VLN;


#define WITH_QWORD(INDEX, NO_SEEK, NAME, ACTION)													\
{																									\
	CSphIndex_VLN * INDEX##pIndex = (CSphIndex_VLN *)INDEX;												\
	DWORD INDEX##uInlineHits = INDEX##pIndex->m_tSettings.m_eHitFormat==SPH_HIT_FORMAT_INLINE;					\
	DWORD INDEX##uInlineDocinfo = INDEX##pIndex->m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE;						\
																									\
	switch ( ( INDEX##uInlineHits<<1 ) | INDEX##uInlineDocinfo )													\
	{																								\
		case 0: { typedef DiskIndexQword_c < false, false, NO_SEEK > NAME; ACTION; break; }			\
		case 1: { typedef DiskIndexQword_c < false, true, NO_SEEK > NAME; ACTION; break; }			\
		case 2: { typedef DiskIndexQword_c < true, false, NO_SEEK > NAME; ACTION; break; }			\
		case 3: { typedef DiskIndexQword_c < true, true, NO_SEEK > NAME; ACTION; break; }			\
		default:																					\
			sphDie ( "INTERNAL ERROR: impossible qword settings" );									\
	}																								\
}

}