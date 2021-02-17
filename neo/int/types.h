#pragma once

#include <cassert>
#include <climits>
#include <ctype.h>
#include <cstdint>

#include "neo/platform/compat.h"
#include "neo/int/string.h"
#include "neo/int/vector.h"
#include "neo/int/variant.h"
#include "neo/int/non_copyable.h"
#include "neo/int/named_int.h"



#if USE_64BIT

	// use 64-bit unsigned integers to store document and word IDs
#define SPHINX_BITS_TAG	"-id64"
	typedef uint64_t		SphWordID_t;
	typedef uint64_t		SphDocID_t;

#define DOCID_MAX		U64C(0xffffffffffffffff)
#define DOCID_FMT		UINT64_FMT
#define DOCINFO_IDSIZE	2

	STATIC_SIZE_ASSERT(SphWordID_t, 8);
	STATIC_SIZE_ASSERT(SphDocID_t, 8);

#else

	// use 32-bit unsigned integers to store document and word IDs
#define SPHINX_BITS_TAG	""
	typedef DWORD			SphWordID_t;
	typedef DWORD			SphDocID_t;

#define DOCID_MAX		0xffffffffUL
#define DOCID_FMT		"%u"
#define DOCINFO_IDSIZE	1

	STATIC_SIZE_ASSERT(SphWordID_t, 4);
	STATIC_SIZE_ASSERT(SphDocID_t, 4);

#endif // USE_64BIT

#define DWSIZEOF(a) ( sizeof(a) / sizeof(DWORD) )


namespace NEO {


	/// row entry (storage only, does not necessarily map 1:1 to attributes)
	typedef DWORD			CSphRowitem;
	typedef const BYTE* CSphRowitemPtr;

	/// widest integer type that can be be stored as an attribute (ideally, fully decoupled from rowitem size!)
	typedef int64_t			SphAttr_t;

	const CSphRowitem		ROWITEM_MAX = UINT_MAX;
	const int				ROWITEM_BITS = 8 * sizeof(CSphRowitem);
	const int				ROWITEMPTR_BITS = 8 * sizeof(CSphRowitemPtr);
	const int				ROWITEM_SHIFT = 5;

	STATIC_ASSERT((1 << ROWITEM_SHIFT) == ROWITEM_BITS, INVALID_ROWITEM_SHIFT);

#ifndef USE_LITTLE_ENDIAN
#error Please define endianness
#endif


#if USE_WINDOWS
	typedef __int64				SphOffset_t;
#define STDOUT_FILENO		_fileno(stdout)
#define STDERR_FILENO		_fileno(stderr)
#else
	typedef off_t				SphOffset_t;
#endif

	STATIC_SIZE_ASSERT(SphOffset_t, 8);

	//hitman
	/// hit position storage type
	typedef DWORD Hitpos_t;

	/// empty hit value
	#define EMPTY_HIT 0

}