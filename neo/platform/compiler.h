#pragma once
#include "neo/core/config.h"
#include "neo/index/enums.h"

#include <cstdint>

/////////////////////////////////////////////////////////////////////////////
// COMPILE-TIME CHECKS
/////////////////////////////////////////////////////////////////////////////


	#if defined (__GNUC__)
	#define SPH_UNUSED_ATTR __attribute__((unused))
	#else
	#define  SPH_UNUSED_ATTR
	#endif


#define STATIC_ASSERT(_cond,_name)		typedef char STATIC_ASSERT_FAILED_ ## _name [ (_cond) ? 1 : -1 ] SPH_UNUSED_ATTR
#define STATIC_SIZE_ASSERT(_type,_size)	STATIC_ASSERT ( sizeof(_type)==_size, _type ## _MUST_BE_ ## _size ## _BYTES )


#ifndef __analysis_assume
#define __analysis_assume(_arg)
#endif


/// some function arguments only need to have a name in debug builds
#ifndef NDEBUG
#define DEBUGARG(_arg) _arg
#else
#define DEBUGARG(_arg)
#endif



/////////////////////////////////////////////////////////////////////////////
// DEBUGGING
/////////////////////////////////////////////////////////////////////////////

#if USE_WINDOWS
#ifndef NDEBUG

void sphAssert(const char* sExpr, const char* sFile, int iLine);

#undef assert
#define assert(_expr) (void)( (_expr) || ( sphAssert ( #_expr, __FILE__, __LINE__ ), 0 ) )

#endif // !NDEBUG
#endif // USE_WINDOWS


// to avoid disappearing of _expr in release builds
#ifndef NDEBUG
#define Verify(_expr) assert(_expr)
#else
#define Verify(_expr) _expr
#endif

/////////////////////////////////////

#if USE_WINDOWS
#ifndef NDEBUG

void sphAssert(const char* sExpr, const char* sFile, int iLine)
{
	char sBuffer[1024];
	_snprintf(sBuffer, sizeof(sBuffer), "%s(%d): assertion %s failed\n", sFile, iLine, sExpr);

	if (MessageBox(NULL, sBuffer, "Assert failed! Cancel to debug.",
		MB_OKCANCEL | MB_TOPMOST | MB_SYSTEMMODAL | MB_ICONEXCLAMATION) != IDOK)
	{
		__debugbreak();
	}
	else
	{
		fprintf(stdout, "%s", sBuffer);
		exit(1);
	}
}

#endif // !NDEBUG
#endif // USE_WINDOWS


/////////////////////////////////////////////////////////////////////////////
// 64-BIT INTEGER TYPES AND MACROS
/////////////////////////////////////////////////////////////////////////////


#if defined(U64C) || defined(I64C)
#error "Internal 64-bit integer macros already defined."
#endif

#if !HAVE_STDINT_H

#if defined(_MSC_VER)
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#define U64C(v) v ## UI64
#define I64C(v) v ## I64
#define PRIu64 "I64d"
#define PRIi64 "I64d"
#else // !defined(_MSC_VER)
typedef long long int64_t;
typedef unsigned long long uint64_t;
#endif // !defined(_MSC_VER)

#endif // no stdint.h

// if platform-specific macros were not supplied, use common defaults
#ifndef U64C
#define U64C(v) v ## ULL
#endif

#ifndef I64C
#define I64C(v) v ## LL
#endif

#ifndef PRIu64
#define PRIu64 "llu"
#endif

#ifndef PRIi64
#define PRIi64 "lld"
#endif

#define UINT64_FMT "%" PRIu64
#define INT64_FMT "%" PRIi64

#ifndef UINT64_MAX
#define UINT64_MAX U64C(0xffffffffffffffff)
#endif

#ifndef INT64_MIN
#define INT64_MIN I64C(0x8000000000000000)
#endif

#ifndef INT64_MAX
#define INT64_MAX I64C(0x7fffffffffffffff)
#endif

STATIC_SIZE_ASSERT(uint64_t, 8);
STATIC_SIZE_ASSERT(int64_t, 8);

// conversion macros that suppress %lld format warnings vs printf
// problem is, on 64-bit Linux systems with gcc and stdint.h, int64_t is long int
// and despite sizeof(long int)==sizeof(long long int)==8, gcc bitches about that
// using PRIi64 instead of %lld is of course The Right Way, but ugly like fuck
// so lets wrap them args in INT64() instead
#define INT64(_v) ((long long int)(_v))
#define UINT64(_v) ((unsigned long long int)(_v))

/////////////////////////////////////////////////////////////////////////////
// MEMORY MANAGEMENT
/////////////////////////////////////////////////////////////////////////////

#define SPH_DEBUG_LEAKS			0
#define SPH_ALLOC_FILL			0
#define SPH_ALLOCS_PROFILER		0
#define SPH_DEBUG_BACKTRACES 0 // will add not only file/line, but also full backtrace

#if SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER

/// debug new that tracks memory leaks
void* operator new (size_t iSize, const char* sFile, int iLine);

/// debug new that tracks memory leaks
void* operator new[](size_t iSize, const char* sFile, int iLine);

/// get current allocs count
int				sphAllocsCount();

/// total allocated bytes
int64_t			sphAllocBytes();

/// get last alloc id
int				sphAllocsLastID();

/// dump all allocs since given id
void			sphAllocsDump(int iFile, int iSinceID);

/// dump stats to stdout
void			sphAllocsStats();

/// check all existing allocs; raises assertion failure in cases of errors
void			sphAllocsCheck();

void			sphMemStatDump(int iFD);

void			sphMemStatMMapAdd(int64_t iSize);
void			sphMemStatMMapDel(int64_t iSize);

#undef new
#define new		new(__FILE__,__LINE__)

#if USE_RE2
void			operator delete (void* pPtr) throw ();
void			operator delete[](void* pPtr) throw ();
#else
/// delete for my new
void			operator delete (void* pPtr);

/// delete for my new
void			operator delete[](void* pPtr);
#endif
#endif // SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER

