#pragma once
#include "neo/int/types.h"

namespace NEO {

#if PARANOID

#define SPH_VARINT_DECODE(_type,_getexpr) \
	register DWORD b = 0; \
	register _type v = 0; \
	int it = 0; \
	do { b = _getexpr; v = ( v<<7 ) + ( b&0x7f ); it++; } while ( b&0x80 ); \
	assert ( (it-1)*7<=sizeof(_type)*8 ); \
	return v;

#else

#define SPH_VARINT_DECODE(_type,_getexpr) \
	register DWORD b = _getexpr; \
	register _type res = 0; \
	while ( b & 0x80 ) \
	{ \
		res = ( res<<7 ) + ( b & 0x7f ); \
		b = _getexpr; \
	} \
	res = ( res<<7 ) + b; \
	return res;

#endif // PARANOID


	DWORD sphUnzipInt(const BYTE*& pBuf);
	SphOffset_t sphUnzipOffset(const BYTE*& pBuf);

#if USE_64BIT
#define sphUnzipWordid sphUnzipOffset
#else
#define sphUnzipWordid sphUnzipInt
#endif


}